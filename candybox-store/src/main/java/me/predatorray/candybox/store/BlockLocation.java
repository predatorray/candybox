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

/**
 * A number pair defining the location of a super-block.
 *
 * <p>An instance of this class may be used to locate a sub-part of a super-block, usually a {@link CandyBlock}.</p>
 *
 * @author Wenhao Ji
 */
public class BlockLocation {

    private final long offset;
    private final long length;

    /**
     * Construct a block location instance by its starting offset and block length.
     * @param offset the starting offset of the block
     * @param length how long the block has
     * @throws IllegalArgumentException if the offset is negative or the length is not positive.
     *                                  (zero length is permitted)
     */
    public BlockLocation(long offset, long length) {
        this.offset = Validations.nonnegative(offset);
        this.length = Validations.positive(length);
    }

    /**
     * @return the starting offset of the block
     */
    public long getOffset() {
        return offset;
    }

    /**
     * @return the length of the block
     */
    public long getLength() {
        return length;
    }

    /**
     * @return the starting offset of next block, which equals {@code offset + length}
     */
    public long getNextOffset() {
        return offset + length;
    }

    /**
     * Determine if the location is out of the range of the super-block where it belongs.
     *
     * <p>This method returns {@code true}, if anyone of the conditions below is true:
     * <ol>
     *     <li>The starting offset is greater than or equal to the size of the super-block;</li>
     *     <li>The offset of the block's end, which equals {@code offset + length} is greater than
     *     the size of the super-block</li>
     * </ol>
     *
     * @param superBlockTotalLength the length of the super-block where this block belongs
     * @return {@code true} if the location is out of the range of the super-block where it belongs,
     *         otherwise {@code false}
     */
    public boolean isOutOfRange(long superBlockTotalLength) {
        return (offset + length) > superBlockTotalLength;
    }

    /**
     * Two instances of this class equal if and only if the offset and the length of the two is equal.
     *
     * {@inheritDoc}
     */
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
