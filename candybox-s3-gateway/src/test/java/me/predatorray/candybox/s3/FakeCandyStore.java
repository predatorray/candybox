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
