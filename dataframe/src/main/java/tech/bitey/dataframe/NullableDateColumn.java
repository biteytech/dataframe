/*
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
import java.time.LocalDate;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableDateColumn extends NullableIntArrayColumn<LocalDate, DateColumn, NonNullDateColumn, NullableDateColumn> implements DateColumn {
	
	static final NullableDateColumn EMPTY = new NullableDateColumn(NonNullDateColumn.EMPTY.get(NONNULL_CHARACTERISTICS), EMPTY_BITSET, null, 0, 0);

	NullableDateColumn(NonNullDateColumn column, BufferBitSet nonNulls, IntBuffer nullCounts, int offset, int size) {
		super(column, nonNulls, nullCounts, offset, size);
	}

	@Override
	NullableDateColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableDateColumn(column, nonNulls, nullCounts, fromIndex+offset, toIndex-fromIndex);
	}

	@Override
	NullableDateColumn empty() {
		return EMPTY;
	}

	@Override
	public int yyyymmdd(int index) {
		checkGetPrimitive(index);
		return column.yyyymmdd(nonNullIndex(index+offset));
	}

	@Override
	NullableDateColumn construct(NonNullDateColumn column, BufferBitSet nonNulls, int size) {
		return new NullableDateColumn(column, nonNulls, null, 0, size);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof LocalDate;
	}
}
