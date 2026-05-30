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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class S3RequestXmlTest {

    private static S3RequestXml.DeleteRequest parse(String xml) {
        return S3RequestXml.parseDelete(xml.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parsesKeysAndDefaultsToNonQuiet() {
        S3RequestXml.DeleteRequest r = parse(
                "<Delete><Object><Key>a.txt</Key></Object>"
                        + "<Object><Key>b/c.txt</Key></Object></Delete>");
        assertThat(r.keys()).containsExactly("a.txt", "b/c.txt");
        assertThat(r.quiet()).isFalse();
    }

    @Test
    void unescapesEntitiesInKeys() {
        S3RequestXml.DeleteRequest r = parse(
                "<Delete><Object><Key>a &amp; b &lt;c&gt;.txt</Key></Object></Delete>");
        assertThat(r.keys()).containsExactly("a & b <c>.txt");
    }

    @Test
    void readsQuietFlag() {
        S3RequestXml.DeleteRequest r = parse(
                "<Delete><Quiet>true</Quiet><Object><Key>k</Key></Object></Delete>");
        assertThat(r.quiet()).isTrue();
        assertThat(r.keys()).containsExactly("k");
    }

    @Test
    void malformedBodyIsRejected() {
        assertThatThrownBy(() -> parse("<Delete><Object>"))
                .isInstanceOfSatisfying(S3Exception.class,
                        e -> assertThat(e.error()).isEqualTo(S3ErrorCode.MALFORMED_XML));
    }

    @Test
    void rejectsDtdSoExternalEntitiesCannotExpand() {
        // A DOCTYPE/XXE attempt: with DTDs disabled the parser must refuse rather than resolve the
        // external entity.
        String xxe = "<?xml version=\"1.0\"?>"
                + "<!DOCTYPE Delete [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
                + "<Delete><Object><Key>&xxe;</Key></Object></Delete>";
        assertThatThrownBy(() -> parse(xxe))
                .isInstanceOfSatisfying(S3Exception.class,
                        e -> assertThat(e.error()).isEqualTo(S3ErrorCode.MALFORMED_XML));
    }
}
