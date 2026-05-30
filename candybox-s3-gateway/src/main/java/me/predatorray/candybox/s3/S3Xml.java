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
    static String listBucket(String bucket, String prefix, String delimiter, int maxKeys,
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
            for (String cp : commonPrefixes) {
                w.writeStartElement("CommonPrefixes");
                el(w, "Prefix", cp);
                w.writeEndElement();
            }
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
