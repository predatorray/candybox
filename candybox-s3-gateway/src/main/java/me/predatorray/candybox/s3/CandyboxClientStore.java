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

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.client.CandyboxClient.CandyInfo;
import me.predatorray.candybox.client.CandyboxClient.Listing;
import me.predatorray.candybox.client.CandyboxClient.MultipartListing;
import me.predatorray.candybox.client.CandyboxClient.PartListing;
import me.predatorray.candybox.client.CandyboxClient.PartUploadInfo;
import me.predatorray.candybox.client.CandyboxClient.RangeBytes;

/**
 * The production {@link CandyStore}: delegates to a cluster-aware {@link CandyboxClient}. Object writes
 * pass a {@code null} idempotency token (S3 PUT has no such concept). The gateway owns the client's
 * lifecycle and closes it on shutdown.
 */
final class CandyboxClientStore implements CandyStore, AutoCloseable {

    private final CandyboxClient client;

    CandyboxClientStore(CandyboxClient client) {
        this.client = client;
    }

    @Override
    public void createBox(String box) {
        client.createBox(box);
    }

    @Override
    public void deleteBox(String box) {
        client.deleteBox(box, false);
    }

    @Override
    public boolean headBox(String box) {
        return client.headBox(box);
    }

    @Override
    public List<String> listBoxes() {
        return client.listBoxes();
    }

    @Override
    public void putCandy(String box, String key, byte[] data, String contentType,
                         Map<String, String> userMetadata) {
        client.putCandy(box, key, data, contentType, userMetadata, null);
    }

    @Override
    public void putCandy(String box, String key, byte[] data, String contentType,
                         Map<String, String> userMetadata, String owner,
                         List<String> grants) {
        client.putCandy(box, key, data, contentType, userMetadata, null, owner, grants);
    }

    @Override
    public CandyInfo copyCandy(String box, String srcKey, String dstKey, String owner,
                               List<String> grants) {
        return client.copyCandy(box, srcKey, dstKey, null, owner, grants);
    }

    @Override
    public CandyInfo completeMultipartUpload(String box, String key, String uploadId,
                                             List<PartUploadInfo> parts, String owner,
                                             List<String> grants) {
        return client.completeMultipartUpload(box, key, uploadId, parts, null, owner, grants);
    }

    @Override
    public java.util.Optional<me.predatorray.candybox.common.auth.BoxAcl> getBoxAcl(String box) {
        return client.getBoxAcl(box);
    }

    @Override
    public void setBoxAcl(String box, me.predatorray.candybox.common.auth.BoxAcl acl) {
        client.setBoxAcl(box, acl);
    }

    @Override
    public me.predatorray.candybox.common.auth.ObjectAcl getCandyAcl(String box, String key) {
        return client.getCandyAcl(box, key);
    }

    @Override
    public void setCandyAcl(String box, String key,
                            me.predatorray.candybox.common.auth.ObjectAcl acl) {
        client.setCandyAcl(box, key, acl);
    }

    @Override
    public byte[] getCandy(String box, String key) {
        return client.getCandy(box, key);
    }

    @Override
    public RangeBytes getCandyRange(String box, String key, long firstByte, long lastByte) {
        return client.getCandyRange(box, key, firstByte, lastByte);
    }

    @Override
    public CandyInfo headCandy(String box, String key) {
        return client.headCandy(box, key);
    }

    @Override
    public void deleteCandy(String box, String key) {
        client.deleteCandy(box, key);
    }

    @Override
    public CandyInfo copyCandy(String box, String srcKey, String dstKey) {
        return client.copyCandy(box, srcKey, dstKey, null);
    }

    @Override
    public Listing listCandies(String box, String prefix, String startAfter, int maxKeys) {
        return client.listCandies(box, prefix, startAfter, maxKeys);
    }

    @Override
    public String createMultipartUpload(String box, String key, String contentType,
                                        Map<String, String> userMetadata) {
        return client.createMultipartUpload(box, key, contentType, userMetadata);
    }

    @Override
    public PartUploadInfo uploadPart(String box, String key, String uploadId, int partNumber,
                                     byte[] data) {
        return client.uploadPart(box, key, uploadId, partNumber, data);
    }

    @Override
    public PartUploadInfo uploadPartCopy(String box, String key, String uploadId, int partNumber,
                                         String srcKey, long firstByte, long lastByte) {
        return client.uploadPartCopy(box, key, uploadId, partNumber, srcKey, firstByte, lastByte);
    }

    @Override
    public CandyInfo completeMultipartUpload(String box, String key, String uploadId,
                                             List<PartUploadInfo> parts) {
        return client.completeMultipartUpload(box, key, uploadId, parts, null);
    }

    @Override
    public void abortMultipartUpload(String box, String key, String uploadId) {
        client.abortMultipartUpload(box, key, uploadId);
    }

    @Override
    public MultipartListing listMultipartUploads(String box, String prefix, String keyMarker,
                                                 String uploadIdMarker, int maxUploads) {
        return client.listMultipartUploads(box, prefix, keyMarker, uploadIdMarker, maxUploads);
    }

    @Override
    public PartListing listParts(String box, String key, String uploadId, int partNumberMarker,
                                 int maxParts) {
        return client.listParts(box, key, uploadId, partNumberMarker, maxParts);
    }

    @Override
    public void close() {
        client.close();
    }
}
