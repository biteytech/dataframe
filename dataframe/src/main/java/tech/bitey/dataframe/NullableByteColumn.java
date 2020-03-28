/*
 * Copyright 2020 biteytech@protonmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.bitey.dataframe;

import static tech.bitey.bufferstuff.BufferBitSet.EMPTY_BITSET;
import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.nio.IntBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableByteColumn extends NullableByteArrayColumn<Byte, ByteColumn, NonNullByteColumn, NullableByteColumn>
		implements ByteColumn {

	static final NullableByteColumn EMPTY = new NullableByteColumn(NonNullByteColumn.EMPTY.get(NONNULL_CHARACTERISTICS),
			EMPTY_BITSET, null, 0, 0);

	NullableByteColumn(NonNullByteColumn column, BufferBitSet nonNulls, IntBuffer nullCounts, int offset, int size) {
		super(column, nonNulls, nullCounts, offset, size);
	}

	@Override
	NullableByteColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableByteColumn(column, nonNulls, nullCounts, fromIndex + offset, toIndex - fromIndex);
	}

	@Override
	NullableByteColumn empty() {
		return EMPTY;
	}

	@Override
	public double min() {
		return subColumn.min();
	}

	@Override
	public double max() {
		return subColumn.max();
	}

	@Override
	public double mean() {
		return subColumn.mean();
	}

	@Override
	public double stddev(boolean population) {
		return subColumn.stddev(population);
	}

	@Override
	public byte getByte(int index) {
		checkGetPrimitive(index);
		return column.getByte(nonNullIndex(index + offset));
	}

	@Override
	NullableByteColumn construct(NonNullByteColumn column, BufferBitSet nonNulls, int size) {
		return new NullableByteColumn(column, nonNulls, null, 0, size);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Byte;
	}
}
