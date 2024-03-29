/*
 * Copyright 2022 biteytech@protonmail.com
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

import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableIntColumn extends NullableIntArrayColumn<Integer, IntColumn, NonNullIntColumn, NullableIntColumn>
		implements IntColumn {

	NullableIntColumn(NonNullColumn<Integer, IntColumn, NonNullIntColumn> column, BufferBitSet nonNulls,
			INullCounts nullCounts, int offset, int size) {
		super((NonNullIntColumn) column, nonNulls, nullCounts, offset, size);
	}

	@Override
	public int getInt(int index) {
		checkGetPrimitive(index);
		return column.getInt(nonNullIndex(index + offset));
	}

	@Override
	public IntStream intStream() {
		return subColumn.intStream();
	}

	@Override
	public IntColumn cleanInt(IntPredicate predicate) {

		BufferBitSet cleanNonNulls = new BufferBitSet();
		IntColumn cleaned = subColumn.cleanInt(predicate, cleanNonNulls);

		return clean(cleaned, cleanNonNulls);
	}

	@Override
	public IntColumn filterInt(IntPredicate predicate, boolean keepNulls) {

		BufferBitSet keep = new BufferBitSet();
		var filtered = subColumn.filterInt0(predicate, keep);

		return filter(filtered, keep, keepNulls);
	}

	@Override
	public IntColumn evaluate(IntUnaryOperator op) {

		return new NullableIntColumn((NonNullIntColumn) subColumn.evaluate(op), subNonNulls(), null, 0, size);
	}
}
