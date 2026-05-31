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

import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Renders the S3 XML response bodies the gateway returns, using the JDK StAX writer (so element text
 * and attributes are escaped by the library, not by hand). The S3 namespace
 * {@code http://s3.amazonaws.com/doc/2006-03-01/} is declared as the default namespace on the document
 * roots that use it ({@code Error} and {@code CopyObjectResult} are unnamespaced, matching S3).
 */
final class S3Xml {

    static final String NS = "http://s3.amazonaws.com/doc/2006-03-01/";
    private static final String XSI_NS = "http://www.w3.org/2001/XMLSchema-instance";
    private static final String ANON_ID = "candybox";

    private static final XMLOutputFactory OUTPUT_FACTORY = XMLOutputFactory.newFactory();
    private static final DateTimeFormatter ISO8601 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private S3Xml() {
    }

    /** An object row in a listing. {@code etag} may be {@code null} (the listing API returns no CRC32C). */
    record Content(String key, long size, long lastModifiedMillis, String etag) {
    }

    static String error(S3ErrorCode error, String message, String resource, String requestId) {
        return doc(w -> {
            w.writeStartElement("Error");
            el(w, "Code", error.code());
            el(w, "Message", message == null ? error.defaultMessage() : message);
            if (resource != null) {
                el(w, "Resource", resource);
            }
            el(w, "RequestId", requestId);
            w.writeEndElement();
        });
    }

    static String listAllMyBuckets(List<String> buckets) {
        String created = ISO8601.format(Instant.EPOCH);
        return doc(w -> {
            root(w, "ListAllMyBucketsResult");
            w.writeStartElement("Owner");
            el(w, "ID", ANON_ID);
            el(w, "DisplayName", ANON_ID);
            w.writeEndElement();
            w.writeStartElement("Buckets");
            for (String b : buckets) {
                w.writeStartElement("Bucket");
                el(w, "Name", b);
                el(w, "CreationDate", created);
                w.writeEndElement();
            }
            w.writeEndElement();
            w.writeEndElement();
        });
    }

    /** ListObjectsV2 result. {@code nextContinuationToken} non-null iff truncated. */
    static String listBucketV2(String bucket, String prefix, String delimiter, int maxKeys,
                               List<Content> contents, List<String> commonPrefixes,
                               String continuationToken, String nextContinuationToken,
                               String startAfter) {
        return doc(w -> {
            root(w, "ListBucketResult");
            el(w, "Name", bucket);
            el(w, "Prefix", prefix == null ? "" : prefix);
            if (continuationToken != null) {
                el(w, "ContinuationToken", continuationToken);
            }
            if (startAfter != null) {
                el(w, "StartAfter", startAfter);
            }
            if (delimiter != null) {
                el(w, "Delimiter", delimiter);
            }
            el(w, "KeyCount", Integer.toString(contents.size() + commonPrefixes.size()));
            el(w, "MaxKeys", Integer.toString(maxKeys));
            boolean truncated = nextContinuationToken != null;
            el(w, "IsTruncated", Boolean.toString(truncated));
            if (truncated) {
                el(w, "NextContinuationToken", nextContinuationToken);
            }
            writeContents(w, contents);
            writeCommonPrefixes(w, commonPrefixes);
            w.writeEndElement();
        });
    }

    /**
     * ListObjects (V1) result. Differs from V2 only in its pagination cursor: it echoes {@code <Marker>}
     * and, when a delimiter rolls keys up (matching S3, which omits {@code NextMarker} otherwise),
     * emits {@code <NextMarker>}; flat listings page from the last returned key. No {@code KeyCount}
     * (that element is V2-only).
     */
    static String listBucketV1(String bucket, String prefix, String delimiter, int maxKeys,
                               List<Content> contents, List<String> commonPrefixes,
                               String marker, String nextMarker, boolean truncated) {
        return doc(w -> {
            root(w, "ListBucketResult");
            el(w, "Name", bucket);
            el(w, "Prefix", prefix == null ? "" : prefix);
            el(w, "Marker", marker == null ? "" : marker);
            if (nextMarker != null) {
                el(w, "NextMarker", nextMarker);
            }
            if (delimiter != null) {
                el(w, "Delimiter", delimiter);
            }
            el(w, "MaxKeys", Integer.toString(maxKeys));
            el(w, "IsTruncated", Boolean.toString(truncated));
            writeContents(w, contents);
            writeCommonPrefixes(w, commonPrefixes);
            w.writeEndElement();
        });
    }

    /**
     * ListObjectVersions result for the unversioned store: every object surfaces as a single latest
     * {@code <Version>} carrying the literal {@code null} version id (S3's convention for an object in
     * an unversioned bucket), so version-aware clients — notably the s3-tests cleanup, which enumerates
     * a bucket via {@code list_object_versions} before deleting — can drain it. Paginated by key-marker;
     * the version-id marker is always empty since there is only one version per key.
     */
    static String listVersions(String bucket, String prefix, String delimiter, int maxKeys,
                               List<Content> versions, List<String> commonPrefixes,
                               String keyMarker, String nextKeyMarker) {
        return doc(w -> {
            root(w, "ListVersionsResult");
            el(w, "Name", bucket);
            el(w, "Prefix", prefix == null ? "" : prefix);
            el(w, "KeyMarker", keyMarker == null ? "" : keyMarker);
            el(w, "VersionIdMarker", "");
            boolean truncated = nextKeyMarker != null;
            if (truncated) {
                el(w, "NextKeyMarker", nextKeyMarker);
                el(w, "NextVersionIdMarker", "");
            }
            if (delimiter != null) {
                el(w, "Delimiter", delimiter);
            }
            el(w, "MaxKeys", Integer.toString(maxKeys));
            el(w, "IsTruncated", Boolean.toString(truncated));
            for (Content c : versions) {
                w.writeStartElement("Version");
                el(w, "Key", c.key());
                el(w, "VersionId", "null");
                el(w, "IsLatest", "true");
                el(w, "LastModified", ISO8601.format(Instant.ofEpochMilli(c.lastModifiedMillis())));
                if (c.etag() != null) {
                    el(w, "ETag", '"' + c.etag() + '"');
                }
                el(w, "Size", Long.toString(c.size()));
                el(w, "StorageClass", "STANDARD");
                w.writeEndElement();
            }
            writeCommonPrefixes(w, commonPrefixes);
            w.writeEndElement();
        });
    }

    /** Response body for {@code POST /b/k?uploads}. */
    static String initiateMultipartUploadResult(String bucket, String key, String uploadId) {
        return doc(w -> {
            root(w, "InitiateMultipartUploadResult");
            el(w, "Bucket", bucket);
            el(w, "Key", key);
            el(w, "UploadId", uploadId);
            w.writeEndElement();
        });
    }

    /** Response body for {@code POST /b/k?uploadId=…}. */
    static String completeMultipartUploadResult(String bucket, String key, String location,
                                                String etag) {
        return doc(w -> {
            root(w, "CompleteMultipartUploadResult");
            el(w, "Location", location);
            el(w, "Bucket", bucket);
            el(w, "Key", key);
            el(w, "ETag", '"' + etag + '"');
            w.writeEndElement();
        });
    }

    /** Response body for {@code GET /b/?uploads}. */
    static String listMultipartUploadsResult(String bucket, String prefix, String keyMarker,
                                             String uploadIdMarker, int maxUploads,
                                             boolean truncated, String nextKeyMarker,
                                             String nextUploadIdMarker,
                                             List<UploadRow> uploads) {
        return doc(w -> {
            root(w, "ListMultipartUploadsResult");
            el(w, "Bucket", bucket);
            if (keyMarker != null) {
                el(w, "KeyMarker", keyMarker);
            } else {
                el(w, "KeyMarker", "");
            }
            if (uploadIdMarker != null) {
                el(w, "UploadIdMarker", uploadIdMarker);
            } else {
                el(w, "UploadIdMarker", "");
            }
            if (nextKeyMarker != null) {
                el(w, "NextKeyMarker", nextKeyMarker);
            }
            if (nextUploadIdMarker != null) {
                el(w, "NextUploadIdMarker", nextUploadIdMarker);
            }
            if (prefix != null && !prefix.isEmpty()) {
                el(w, "Prefix", prefix);
            }
            el(w, "MaxUploads", Integer.toString(maxUploads));
            el(w, "IsTruncated", Boolean.toString(truncated));
            for (UploadRow u : uploads) {
                w.writeStartElement("Upload");
                el(w, "Key", u.key());
                el(w, "UploadId", u.uploadId());
                el(w, "Initiated", ISO8601.format(Instant.ofEpochMilli(u.createdAtMillis())));
                w.writeEndElement();
            }
            w.writeEndElement();
        });
    }

    /** Response body for {@code GET /b/k?uploadId=…}. */
    static String listPartsResult(String bucket, String key, String uploadId, int partNumberMarker,
                                  int maxParts, boolean truncated, int nextPartNumberMarker,
                                  List<PartRow> parts) {
        return doc(w -> {
            root(w, "ListPartsResult");
            el(w, "Bucket", bucket);
            el(w, "Key", key);
            el(w, "UploadId", uploadId);
            el(w, "PartNumberMarker", Integer.toString(partNumberMarker));
            if (truncated) {
                el(w, "NextPartNumberMarker", Integer.toString(nextPartNumberMarker));
            }
            el(w, "MaxParts", Integer.toString(maxParts));
            el(w, "IsTruncated", Boolean.toString(truncated));
            for (PartRow p : parts) {
                w.writeStartElement("Part");
                el(w, "PartNumber", Integer.toString(p.partNumber()));
                el(w, "ETag", '"' + p.etag() + '"');
                el(w, "Size", Long.toString(p.size()));
                w.writeEndElement();
            }
            w.writeEndElement();
        });
    }

    /** One row inside a {@link #listMultipartUploadsResult}. */
    record UploadRow(String key, String uploadId, long createdAtMillis) {
    }

    /** One row inside a {@link #listPartsResult}. */
    record PartRow(int partNumber, String etag, long size) {
    }

    /** Response body for {@code PUT /b/k?partNumber=…} with {@code x-amz-copy-source}. */
    static String copyPartResult(String etag, long lastModifiedMillis) {
        return doc(w -> {
            root(w, "CopyPartResult");
            el(w, "LastModified", ISO8601.format(Instant.ofEpochMilli(lastModifiedMillis)));
            el(w, "ETag", '"' + etag + '"');
            w.writeEndElement();
        });
    }

    static String copyObjectResult(String etag, long lastModifiedMillis) {
        return doc(w -> {
            w.writeStartElement("CopyObjectResult");
            el(w, "LastModified", ISO8601.format(Instant.ofEpochMilli(lastModifiedMillis)));
            el(w, "ETag", '"' + etag + '"');
            w.writeEndElement();
        });
    }

    /** Batch-delete result. {@code errors} entries are {@code [key, code, message]} triples. */
    static String deleteResult(List<String> deletedKeys, List<String[]> errors) {
        return doc(w -> {
            root(w, "DeleteResult");
            for (String key : deletedKeys) {
                w.writeStartElement("Deleted");
                el(w, "Key", key);
                w.writeEndElement();
            }
            for (String[] e : errors) {
                w.writeStartElement("Error");
                el(w, "Key", e[0]);
                el(w, "Code", e[1]);
                el(w, "Message", e[2]);
                w.writeEndElement();
            }
            w.writeEndElement();
        });
    }

    static String locationConstraint(String region) {
        // S3 returns an empty body for us-east-1; any other region as the element text.
        String body = (region == null || region.equals("us-east-1")) ? "" : region;
        return doc(w -> {
            root(w, "LocationConstraint");
            if (!body.isEmpty()) {
                w.writeCharacters(body);
            }
            w.writeEndElement();
        });
    }

    static String versioningConfiguration() {
        return doc(w -> {
            w.writeStartElement("VersioningConfiguration");
            w.writeDefaultNamespace(NS);
            w.writeEndElement();
        });
    }

    static String accessControlPolicy() {
        return doc(w -> {
            root(w, "AccessControlPolicy");
            w.writeStartElement("Owner");
            el(w, "ID", ANON_ID);
            el(w, "DisplayName", ANON_ID);
            w.writeEndElement();
            w.writeStartElement("AccessControlList");
            w.writeStartElement("Grant");
            w.writeStartElement("Grantee");
            w.writeNamespace("xsi", XSI_NS);
            w.writeAttribute(XSI_NS, "type", "CanonicalUser");
            el(w, "ID", ANON_ID);
            el(w, "DisplayName", ANON_ID);
            w.writeEndElement();
            el(w, "Permission", "FULL_CONTROL");
            w.writeEndElement();
            w.writeEndElement();
            w.writeEndElement();
        });
    }

    // ---- internals -------------------------------------------------------------------------

    /** Writes the {@code <Contents>} rows shared by the V1 and V2 listing results. */
    private static void writeContents(XMLStreamWriter w, List<Content> contents) throws XMLStreamException {
        for (Content c : contents) {
            w.writeStartElement("Contents");
            el(w, "Key", c.key());
            el(w, "LastModified", ISO8601.format(Instant.ofEpochMilli(c.lastModifiedMillis())));
            if (c.etag() != null) {
                el(w, "ETag", '"' + c.etag() + '"');
            }
            el(w, "Size", Long.toString(c.size()));
            el(w, "StorageClass", "STANDARD");
            w.writeEndElement();
        }
    }

    /** Writes the {@code <CommonPrefixes>} rollup shared by every listing result. */
    private static void writeCommonPrefixes(XMLStreamWriter w, List<String> commonPrefixes)
            throws XMLStreamException {
        for (String cp : commonPrefixes) {
            w.writeStartElement("CommonPrefixes");
            el(w, "Prefix", cp);
            w.writeEndElement();
        }
    }

    /** Opens a document root element in the S3 default namespace. */
    private static void root(XMLStreamWriter w, String name) throws XMLStreamException {
        w.writeStartElement(name);
        w.writeDefaultNamespace(NS);
    }

    /** Writes {@code <name>text</name>}; the writer escapes the text. */
    private static void el(XMLStreamWriter w, String name, String text) throws XMLStreamException {
        w.writeStartElement(name);
        if (text != null) {
            w.writeCharacters(text);
        }
        w.writeEndElement();
    }

    @FunctionalInterface
    private interface Body {
        void write(XMLStreamWriter w) throws XMLStreamException;
    }

    private static String doc(Body body) {
        StringWriter out = new StringWriter();
        try {
            XMLStreamWriter w = OUTPUT_FACTORY.createXMLStreamWriter(out);
            w.writeStartDocument("UTF-8", "1.0");
            body.write(w);
            w.writeEndDocument();
            w.flush();
            w.close();
        } catch (XMLStreamException e) {
            // Generating our own fixed-shape documents should never fail.
            throw new IllegalStateException("Failed to render S3 XML", e);
        }
        return out.toString();
    }
}
