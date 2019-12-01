/*
 * Copyright 2019 biteytech@protonmail.com
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

final class NullableShortColumn extends NullableColumn<Short, ShortColumn, NonNullShortColumn, NullableShortColumn>
		implements ShortColumn {

	static final NullableShortColumn EMPTY = new NullableShortColumn(
			NonNullShortColumn.EMPTY.get(NONNULL_CHARACTERISTICS), EMPTY_BITSET, null, 0, 0);

	NullableShortColumn(NonNullShortColumn column, BufferBitSet nonNulls, IntBuffer nullCounts, int offset, int size) {
		super(column, nonNulls, nullCounts, offset, size);
	}

	@Override
	NullableShortColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableShortColumn(column, nonNulls, nullCounts, fromIndex + offset, toIndex - fromIndex);
	}

	@Override
	NullableShortColumn empty() {
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
	public short getShort(int index) {
		checkGetPrimitive(index);
		return column.getShort(nonNullIndex(index + offset));
	}

	@Override
	NullableShortColumn construct(NonNullShortColumn column, BufferBitSet nonNulls, int size) {
		return new NullableShortColumn(column, nonNulls, null, 0, size);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Short;
	}

	@Override
	void intersectRightSorted(NonNullShortColumn rhs, IntColumnBuilder indices, BufferBitSet keepLeft) {

		for (int i = offset; i <= lastIndex(); i++) {

			if (!nonNulls.get(i))
				continue;

			int rightIndex = rhs.search(column.at(nonNullIndex(i)));
			if (rightIndex >= rhs.offset && rightIndex <= rhs.lastIndex()) {

				indices.add(rightIndex - rhs.offset);
				keepLeft.set(i - offset);
			}
		}
	}
}
