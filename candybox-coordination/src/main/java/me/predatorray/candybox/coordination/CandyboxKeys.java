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
package me.predatorray.candybox.coordination;

/**
 * Canonical coordination key/resource names, shared by the server (which owns Boxes) and the client
 * (which routes to owners) so both agree on where the ownership lease and manifest pointer live.
 */
public final class CandyboxKeys {

    private CandyboxKeys() {
    }

    /** The ownership-lease resource for a Box. The lease holder is the Box's owner. */
    public static String ownerResource(String boxName) {
        return "boxes/" + boxName + "/owner";
    }

    /** The versioned key holding the pointer to a Box's current manifest ledger. */
    public static String manifestKey(String boxName) {
        return "boxes/" + boxName + "/manifest";
    }
}
