/*
 * Copyright 2021 biteytech@protonmail.com
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

import java.util.function.DoublePredicate;
import java.util.stream.DoubleStream;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableDoubleColumn extends NullableColumn<Double, DoubleColumn, NonNullDoubleColumn, NullableDoubleColumn>
		implements DoubleColumn {

	NullableDoubleColumn(NonNullColumn<Double, DoubleColumn, NonNullDoubleColumn> column, BufferBitSet nonNulls,
			INullCounts nullCounts, int offset, int size) {
		super((NonNullDoubleColumn) column, nonNulls, nullCounts, offset, size);
	}

	@Override
	public double getDouble(int index) {
		checkGetPrimitive(index);
		return column.getDouble(nonNullIndex(index + offset));
	}

	@Override
	public DoubleStream doubleStream() {
		return subColumn.doubleStream();
	}

	@Override
	public DoubleColumn cleanDouble(DoublePredicate predicate) {

		BufferBitSet cleanNonNulls = new BufferBitSet();
		DoubleColumn cleaned = subColumn.cleanDouble(predicate, cleanNonNulls);

		if (cleaned == subColumn)
			return this;
		else if (cleaned.isEmpty())
			return construct((NonNullDoubleColumn) cleaned, cleanNonNulls, size);

		NullableDoubleColumn nullableCleaned = (NullableDoubleColumn) cleaned;

		BufferBitSet nonNulls = new BufferBitSet();
		for (int i = offset, j = 0; i <= lastIndex(); i++)
			if (this.nonNulls.get(i) && nullableCleaned.nonNulls.get(j++))
				nonNulls.set(i - offset);

		return construct(nullableCleaned.column, nonNulls, size);
	}
}
