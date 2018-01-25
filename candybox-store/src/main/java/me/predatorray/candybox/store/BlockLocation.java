/*
 * Copyright (c) 2018 the original author or authors.
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

package me.predatorray.candybox.store;

import me.predatorray.candybox.util.Validations;

import java.util.Objects;

public class BlockLocation {

    private final long offset;
    private final long length;

    public BlockLocation(long offset, long length) {
        this.offset = Validations.nonnegative(offset);
        this.length = Validations.positive(length);
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public boolean isOutOfRange(long superBlockTotalLength) {
        return (offset + length) > superBlockTotalLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlockLocation that = (BlockLocation) o;
        return offset == that.offset &&
                length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, length);
    }

    @Override
    public String toString() {
        return "[" + offset + ", " + (offset + length) + ")";
    }
}
