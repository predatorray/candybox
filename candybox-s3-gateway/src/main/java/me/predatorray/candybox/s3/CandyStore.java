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
import java.util.Optional;
import me.predatorray.candybox.client.CandyboxClient.CandyInfo;
import me.predatorray.candybox.client.CandyboxClient.Listing;
import me.predatorray.candybox.client.CandyboxClient.MultipartListing;
import me.predatorray.candybox.client.CandyboxClient.PartListing;
import me.predatorray.candybox.client.CandyboxClient.PartUploadInfo;
import me.predatorray.candybox.client.CandyboxClient.RangeBytes;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.auth.ObjectAcl;

/**
 * The narrow seam the gateway's request handling depends on, exactly the subset of the Candybox client
 * API the S3 surface needs. Production wires {@link CandyboxClientStore} (over the real
 * {@code CandyboxClient}); unit tests pass a hand-written in-memory fake, so the Netty handler can be
 * exercised with {@code EmbeddedChannel} and no sockets. Methods throw the Candybox exception hierarchy,
 * which {@link ErrorMapper} translates to S3 errors.
 */
interface CandyStore {

    void createBox(String box);

    void deleteBox(String box);

    boolean headBox(String box);

    List<String> listBoxes();

    void putCandy(String box, String key, byte[] data, String contentType,
                  Map<String, String> userMetadata);

    /** {@code putCandy} stamping owner/grants (grants in the text form {@code grantee:OP[+OP...]});
     * the default ignores them so auth-unaware fakes keep working. */
    default void putCandy(String box, String key, byte[] data, String contentType,
                          Map<String, String> userMetadata, String owner, List<String> grants) {
        putCandy(box, key, data, contentType, userMetadata);
    }

    byte[] getCandy(String box, String key);

    /**
     * Range GET: returns a byte window of an object. {@code firstByte}/{@code lastByte} follow the
     * HTTP {@code Range:} convention (inclusive on both ends, {@code -1} sentinel for "unbounded").
     */
    RangeBytes getCandyRange(String box, String key, long firstByte, long lastByte);

    CandyInfo headCandy(String box, String key);

    void deleteCandy(String box, String key);

    /** Same-Box server-side copy; returns the destination's metadata. */
    CandyInfo copyCandy(String box, String srcKey, String dstKey);

    default CandyInfo copyCandy(String box, String srcKey, String dstKey, String owner,
                                List<String> grants) {
        return copyCandy(box, srcKey, dstKey);
    }

    Listing listCandies(String box, String prefix, String startAfter, int maxKeys);

    // ---- ACLs ---------------------------------------------------------------------------------

    /** The Box's ACL document, empty when none exists (legacy Box / auth-unaware fake). */
    default Optional<BoxAcl> getBoxAcl(String box) {
        return Optional.empty();
    }

    default void setBoxAcl(String box, BoxAcl acl) {
        throw new UnsupportedOperationException("ACLs are not supported by this store");
    }

    default ObjectAcl getCandyAcl(String box, String key) {
        return ObjectAcl.NONE;
    }

    default void setCandyAcl(String box, String key, ObjectAcl acl) {
        throw new UnsupportedOperationException("ACLs are not supported by this store");
    }

    // ---- multipart upload -------------------------------------------------------------------

    String createMultipartUpload(String box, String key, String contentType,
                                 Map<String, String> userMetadata);

    PartUploadInfo uploadPart(String box, String key, String uploadId, int partNumber, byte[] data);

    PartUploadInfo uploadPartCopy(String box, String key, String uploadId, int partNumber,
                                  String srcKey, long firstByte, long lastByte);

    CandyInfo completeMultipartUpload(String box, String key, String uploadId,
                                      List<PartUploadInfo> parts);

    default CandyInfo completeMultipartUpload(String box, String key, String uploadId,
                                              List<PartUploadInfo> parts, String owner,
                                              List<String> grants) {
        return completeMultipartUpload(box, key, uploadId, parts);
    }

    void abortMultipartUpload(String box, String key, String uploadId);

    MultipartListing listMultipartUploads(String box, String prefix, String keyMarker,
                                          String uploadIdMarker, int maxUploads);

    PartListing listParts(String box, String key, String uploadId, int partNumberMarker, int maxParts);
}
