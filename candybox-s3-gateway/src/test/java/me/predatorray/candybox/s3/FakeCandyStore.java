/*
 * Copyright (c) 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.predatorray.candybox.s3;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import me.predatorray.candybox.client.CandyboxClient.CandyInfo;
import me.predatorray.candybox.client.CandyboxClient.Listing;
import me.predatorray.candybox.client.CandyboxClient.MultipartListing;
import me.predatorray.candybox.client.CandyboxClient.PartListing;
import me.predatorray.candybox.client.CandyboxClient.PartUploadInfo;
import me.predatorray.candybox.client.CandyboxClient.RangeBytes;
import me.predatorray.candybox.client.CandyboxClient.UploadEntry;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotEmptyException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;

/**
 * A hand-written in-memory {@link CandyStore} for unit tests — no sockets, no BookKeeper. Models the
 * Box/Candy semantics and the failure modes the gateway must translate (missing box/key, box exists,
 * box not empty). Keys are kept sorted per Box so listing/pagination behave like the real engine.
 */
final class FakeCandyStore implements CandyStore {

    private record Obj(byte[] data, String contentType, Map<String, String> meta, int crc32c, long created) {
    }

    private final Map<String, TreeMap<String, Obj>> boxes = new LinkedHashMap<>();

    /** Per-Box in-flight uploads: box → uploadId → upload. */
    private final Map<String, LinkedHashMap<String, PendingUpload>> uploads = new LinkedHashMap<>();
    private long uploadCounter = 0;

    private static final class PendingUpload {
        final String key;
        final String contentType;
        final Map<String, String> meta;
        final long createdAtMillis;
        final java.util.TreeMap<Integer, byte[]> parts = new java.util.TreeMap<>();

        PendingUpload(String key, String contentType, Map<String, String> meta, long createdAtMillis) {
            this.key = key;
            this.contentType = contentType;
            this.meta = meta;
            this.createdAtMillis = createdAtMillis;
        }
    }

    @Override
    public void createBox(String box) {
        if (boxes.containsKey(box)) {
            throw new BoxAlreadyExistsException(box);
        }
        boxes.put(box, new TreeMap<>());
    }

    @Override
    public void deleteBox(String box) {
        TreeMap<String, Obj> b = boxes.get(box);
        if (b == null) {
            throw new BoxNotFoundException(box);
        }
        if (!b.isEmpty()) {
            throw new BoxNotEmptyException(box);
        }
        boxes.remove(box);
    }

    @Override
    public boolean headBox(String box) {
        return boxes.containsKey(box);
    }

    @Override
    public List<String> listBoxes() {
        return new ArrayList<>(boxes.keySet());
    }

    @Override
    public void putCandy(String box, String key, byte[] data, String contentType,
                         Map<String, String> userMetadata) {
        box(box).put(key, new Obj(data.clone(), contentType,
                userMetadata == null ? Map.of() : new LinkedHashMap<>(userMetadata),
                Crc32c.of(data), System.currentTimeMillis()));
    }

    @Override
    public byte[] getCandy(String box, String key) {
        return obj(box, key).data().clone();
    }

    @Override
    public RangeBytes getCandyRange(String box, String key, long firstByte, long lastByte) {
        Obj o = obj(box, key);
        long total = o.data().length;
        long resolvedFirst;
        long resolvedLast;
        if (firstByte < 0 && lastByte < 0) {
            throw new IllegalArgumentException("Invalid range: neither bound supplied");
        }
        if (firstByte < 0) {
            long suffix = lastByte;
            if (suffix <= 0) {
                throw new IllegalArgumentException("Suffix range must be positive");
            }
            resolvedFirst = suffix >= total ? 0 : total - suffix;
            resolvedLast = total - 1;
        } else if (lastByte < 0) {
            resolvedFirst = firstByte;
            resolvedLast = total - 1;
        } else {
            resolvedFirst = firstByte;
            resolvedLast = Math.min(lastByte, total - 1);
        }
        if (total == 0 || resolvedFirst >= total || resolvedLast < resolvedFirst) {
            throw new IllegalArgumentException("Range " + firstByte + "-" + lastByte
                    + " not satisfiable");
        }
        int len = (int) (resolvedLast - resolvedFirst + 1);
        byte[] slice = new byte[len];
        System.arraycopy(o.data(), (int) resolvedFirst, slice, 0, len);
        return new RangeBytes(slice, total, len, o.contentType(), o.meta(), o.crc32c());
    }

    @Override
    public CandyInfo headCandy(String box, String key) {
        Obj o = obj(box, key);
        return new CandyInfo(o.data().length, o.contentType(), o.meta(), o.crc32c(), o.created());
    }

    @Override
    public void deleteCandy(String box, String key) {
        TreeMap<String, Obj> b = box(box);
        if (b.remove(key) == null) {
            throw new CandyNotFoundException(box, key);
        }
    }

    @Override
    public CandyInfo copyCandy(String box, String srcKey, String dstKey) {
        Obj src = obj(box, srcKey);
        box(box).put(dstKey, new Obj(src.data(), src.contentType(), src.meta(), src.crc32c(),
                System.currentTimeMillis()));
        return headCandy(box, dstKey);
    }

    @Override
    public Listing listCandies(String box, String prefix, String startAfter, int maxKeys) {
        TreeMap<String, Obj> b = box(box);
        List<Listing.Entry> entries = new ArrayList<>();
        String next = null;
        for (Map.Entry<String, Obj> e : b.entrySet()) {
            String key = e.getKey();
            if (prefix != null && !prefix.isEmpty() && !key.startsWith(prefix)) {
                continue;
            }
            if (startAfter != null && key.compareTo(startAfter) <= 0) {
                continue;
            }
            if (entries.size() == maxKeys) {
                next = entries.get(entries.size() - 1).key();
                break;
            }
            entries.add(new Listing.Entry(key, e.getValue().data().length, e.getValue().created()));
        }
        return new Listing(entries, next);
    }

    @Override
    public String createMultipartUpload(String box, String key, String contentType,
                                        Map<String, String> userMetadata) {
        box(box); // validate bucket exists
        String uploadId = "upl-" + (++uploadCounter);
        uploads.computeIfAbsent(box, k -> new LinkedHashMap<>()).put(uploadId,
                new PendingUpload(key, contentType,
                        userMetadata == null ? Map.of() : new LinkedHashMap<>(userMetadata),
                        System.currentTimeMillis()));
        return uploadId;
    }

    @Override
    public PartUploadInfo uploadPart(String box, String key, String uploadId, int partNumber,
                                     byte[] data) {
        PendingUpload upload = upload(box, uploadId);
        upload.parts.put(partNumber, data.clone());
        return new PartUploadInfo(partNumber, Crc32c.of(data), data.length);
    }

    @Override
    public PartUploadInfo uploadPartCopy(String box, String key, String uploadId, int partNumber,
                                         String srcKey, long firstByte, long lastByte) {
        PendingUpload upload = upload(box, uploadId);
        Obj source = obj(box, srcKey);
        long total = source.data().length;
        long resolvedFirst = firstByte < 0 ? 0 : firstByte;
        long resolvedLast = lastByte < 0 ? total - 1 : Math.min(lastByte, total - 1);
        if (total == 0 || resolvedFirst >= total || resolvedLast < resolvedFirst) {
            throw new IllegalArgumentException("Range " + firstByte + "-" + lastByte
                    + " not satisfiable");
        }
        int len = (int) (resolvedLast - resolvedFirst + 1);
        byte[] slice = new byte[len];
        System.arraycopy(source.data(), (int) resolvedFirst, slice, 0, len);
        upload.parts.put(partNumber, slice);
        return new PartUploadInfo(partNumber, Crc32c.of(slice), slice.length);
    }

    @Override
    public CandyInfo completeMultipartUpload(String box, String key, String uploadId,
                                             List<PartUploadInfo> parts) {
        PendingUpload upload = upload(box, uploadId);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int lastNum = 0;
        for (PartUploadInfo p : parts) {
            if (p.partNumber() <= lastNum) {
                throw new IllegalArgumentException("Parts must be in ascending order");
            }
            lastNum = p.partNumber();
            byte[] bytes = upload.parts.get(p.partNumber());
            if (bytes == null) {
                throw new IllegalArgumentException("Unknown part " + p.partNumber());
            }
            if (Crc32c.of(bytes) != p.crc32c()) {
                throw new IllegalArgumentException("Part CRC mismatch for " + p.partNumber());
            }
            out.write(bytes, 0, bytes.length);
        }
        uploads.get(box).remove(uploadId);
        byte[] full = out.toByteArray();
        box(box).put(key, new Obj(full, upload.contentType, upload.meta, Crc32c.of(full),
                System.currentTimeMillis()));
        return headCandy(box, key);
    }

    @Override
    public void abortMultipartUpload(String box, String key, String uploadId) {
        if (uploads.containsKey(box)) {
            uploads.get(box).remove(uploadId);
        }
    }

    @Override
    public MultipartListing listMultipartUploads(String box, String prefix, String keyMarker,
                                                 String uploadIdMarker, int maxUploads) {
        box(box);
        List<UploadEntry> rows = new ArrayList<>();
        Map<String, PendingUpload> ups = uploads.getOrDefault(box, new LinkedHashMap<>());
        int limit = maxUploads <= 0 ? 1000 : maxUploads;
        String nextKey = null;
        String nextUpl = null;
        for (Map.Entry<String, PendingUpload> e : ups.entrySet()) {
            if (prefix != null && !prefix.isEmpty() && !e.getValue().key.startsWith(prefix)) {
                continue;
            }
            if (keyMarker != null && e.getValue().key.compareTo(keyMarker) < 0) {
                continue;
            }
            if (rows.size() == limit) {
                nextKey = e.getValue().key;
                nextUpl = e.getKey();
                break;
            }
            rows.add(new UploadEntry(e.getKey(), e.getValue().key, e.getValue().createdAtMillis));
        }
        return new MultipartListing(rows, nextKey, nextUpl);
    }

    @Override
    public PartListing listParts(String box, String key, String uploadId, int partNumberMarker,
                                 int maxParts) {
        PendingUpload upload = upload(box, uploadId);
        List<PartUploadInfo> rows = new ArrayList<>();
        int limit = maxParts <= 0 ? 1000 : maxParts;
        int next = 0;
        for (Map.Entry<Integer, byte[]> e : upload.parts.entrySet()) {
            if (e.getKey() <= partNumberMarker) {
                continue;
            }
            if (rows.size() == limit) {
                next = e.getKey() - 1;
                break;
            }
            rows.add(new PartUploadInfo(e.getKey(), Crc32c.of(e.getValue()), e.getValue().length));
        }
        return new PartListing(rows, next);
    }

    private PendingUpload upload(String box, String uploadId) {
        box(box);
        Map<String, PendingUpload> ups = uploads.get(box);
        PendingUpload u = ups == null ? null : ups.get(uploadId);
        if (u == null) {
            throw new CandyNotFoundException(box, uploadId);
        }
        return u;
    }

    private TreeMap<String, Obj> box(String box) {
        TreeMap<String, Obj> b = boxes.get(box);
        if (b == null) {
            throw new BoxNotFoundException(box);
        }
        return b;
    }

    private Obj obj(String box, String key) {
        Obj o = box(box).get(key);
        if (o == null) {
            throw new CandyNotFoundException(box, key);
        }
        return o;
    }
}
