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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Parses the (untrusted) S3 request XML bodies the gateway accepts, using the JDK StAX reader rather
 * than ad-hoc string scanning. The factory is hardened against XXE and entity-expansion attacks by
 * disabling DTDs and external entities — mandatory since the body comes straight from the client.
 *
 * <p>Currently only {@code DeleteObjects} (a {@code POST ?delete} with a {@code <Delete>} body) has a
 * request payload.
 */
final class S3RequestXml {

    /** A {@code DeleteObjects} request: the keys to delete and whether the response should be quiet. */
    record DeleteRequest(List<String> keys, boolean quiet) {
    }

    private static final XMLInputFactory INPUT_FACTORY = hardenedInputFactory();

    private S3RequestXml() {
    }

    private static XMLInputFactory hardenedInputFactory() {
        XMLInputFactory f = XMLInputFactory.newFactory();
        // Defend against XXE / "billion laughs": no DTDs, no external entity resolution.
        f.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        return f;
    }

    /**
     * Parses a {@code <Delete>} body into the keys to delete (and the {@code <Quiet>} flag). Throws
     * {@link S3Exception} with {@link S3ErrorCode#MALFORMED_XML} if the body is not well-formed.
     */
    static DeleteRequest parseDelete(byte[] body) {
        List<String> keys = new ArrayList<>();
        boolean quiet = false;
        XMLStreamReader reader = null;
        try {
            reader = INPUT_FACTORY.createXMLStreamReader(new ByteArrayInputStream(body));
            while (reader.hasNext()) {
                if (reader.next() != XMLStreamConstants.START_ELEMENT) {
                    continue;
                }
                switch (reader.getLocalName()) {
                    case "Key" -> keys.add(reader.getElementText());
                    case "Quiet" -> quiet = Boolean.parseBoolean(reader.getElementText().trim());
                    default -> { /* Delete, Object, and any unknown elements: descend/ignore. */ }
                }
            }
            return new DeleteRequest(keys, quiet);
        } catch (XMLStreamException e) {
            throw new S3Exception(S3ErrorCode.MALFORMED_XML, "Malformed DeleteObjects body", e);
        } finally {
            closeQuietly(reader);
        }
    }

    private static void closeQuietly(XMLStreamReader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (XMLStreamException ignored) {
                // best effort
            }
        }
    }
}
