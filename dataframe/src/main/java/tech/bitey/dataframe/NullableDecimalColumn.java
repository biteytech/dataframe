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

import java.math.BigDecimal;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableDecimalColumn
		extends NullableVarLenColumn<BigDecimal, DecimalColumn, NonNullDecimalColumn, NullableDecimalColumn>
		implements DecimalColumn {

	static final NullableDecimalColumn EMPTY = new NullableDecimalColumn(
			NonNullDecimalColumn.EMPTY.get(NONNULL_CHARACTERISTICS), EMPTY_BITSET, null, 0, 0);

	NullableDecimalColumn(NonNullDecimalColumn column, BufferBitSet nonNulls, INullCounts nullCounts, int offset,
			int size) {
		super(column, nonNulls, nullCounts, offset, size);
	}

	@Override
	NullableDecimalColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableDecimalColumn(column, nonNulls, nullCounts, fromIndex + offset, toIndex - fromIndex);
	}

	@Override
	NullableDecimalColumn empty() {
		return EMPTY;
	}

	@Override
	NullableDecimalColumn construct(NonNullDecimalColumn column, BufferBitSet nonNulls, int size) {
		return new NullableDecimalColumn(column, nonNulls, null, 0, size);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof BigDecimal;
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
}
