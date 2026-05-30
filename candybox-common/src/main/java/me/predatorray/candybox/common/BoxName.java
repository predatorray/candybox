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
package me.predatorray.candybox.common;

import java.util.regex.Pattern;
import me.predatorray.candybox.common.exception.ValidationException;

/**
 * A validated Box name (the S3 "bucket" name). S3-like rules: 3–63 characters, lowercase letters,
 * digits and hyphens only, not starting or ending with a hyphen. The constraints keep names safely
 * encodable in ZooKeeper paths and ledger metadata.
 *
 * @param value the validated name
 */
public record BoxName(String value) implements Comparable<BoxName> {

    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 63;
    private static final Pattern PATTERN = Pattern.compile("[a-z0-9]([a-z0-9-]*[a-z0-9])?");

    public BoxName {
        if (value == null) {
            throw new ValidationException("Box name must not be null");
        }
        int len = value.length();
        if (len < MIN_LENGTH || len > MAX_LENGTH) {
            throw new ValidationException("Box name length must be " + MIN_LENGTH + ".." + MAX_LENGTH
                    + " chars: '" + value + "'");
        }
        if (!PATTERN.matcher(value).matches()) {
            throw new ValidationException("Box name must match [a-z0-9-], not start/end with '-': '"
                    + value + "'");
        }
    }

    public static BoxName of(String value) {
        return new BoxName(value);
    }

    @Override
    public int compareTo(BoxName o) {
        return value.compareTo(o.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
