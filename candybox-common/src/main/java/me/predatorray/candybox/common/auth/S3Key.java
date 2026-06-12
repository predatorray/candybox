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
package me.predatorray.candybox.common.auth;

/**
 * One S3 access-key pair mapped onto a Candybox {@link Principal}. Unlike SASL verifiers the
 * secret must be stored <em>retrievably</em> — SigV4 is an HMAC over the actual secret, so the
 * gateway has to reproduce it (protect the credentials file accordingly).
 *
 * @param accessKeyId the public key id (the {@code Credential=} element of a SigV4 request)
 * @param secretKey   the shared secret
 * @param principal   the identity the key authenticates as
 */
public record S3Key(String accessKeyId, String secretKey, Principal principal) {
}
