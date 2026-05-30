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
package me.predatorray.candybox.protocol;

/**
 * The wire unit: an {@link Opcode} plus an opaque payload. Higher-level {@link Message}s serialize
 * themselves into a frame's payload; {@link FrameCodec} handles the on-wire framing.
 *
 * @param opcode  the opcode
 * @param payload the payload bytes (treat as read-only)
 */
public record Frame(Opcode opcode, byte[] payload) {

    public Frame {
        if (opcode == null) {
            throw new IllegalArgumentException("opcode is required");
        }
        if (payload == null) {
            payload = new byte[0];
        }
    }
}
