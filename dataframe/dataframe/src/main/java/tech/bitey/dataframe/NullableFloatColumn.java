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

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableFloatColumn extends NullableColumn<Float, FloatColumn, NonNullFloatColumn, NullableFloatColumn>
		implements FloatColumn {

	NullableFloatColumn(NonNullColumn<Float, FloatColumn, NonNullFloatColumn> column, BufferBitSet nonNulls,
			INullCounts nullCounts, int offset, int size) {
		super((NonNullFloatColumn) column, nonNulls, nullCounts, offset, size);
	}

	@Override
	public float getFloat(int index) {
		checkGetPrimitive(index);
		return column.getFloat(nonNullIndex(index + offset));
	}

	@Override
	public FloatColumn cleanFloat(FloatPredicate predicate) {

		BufferBitSet cleanNonNulls = new BufferBitSet();
		FloatColumn cleaned = subColumn.cleanFloat(predicate, cleanNonNulls);

		return clean(cleaned, cleanNonNulls);
	}

	@Override
	public FloatColumn filterFloat(FloatPredicate predicate, boolean keepNulls) {

		BufferBitSet keep = new BufferBitSet();
		var filtered = subColumn.filterFloat0(predicate, keep);

		return filter(filtered, keep, keepNulls);
	}

	@Override
	public FloatColumn evaluate(FloatUnaryOperator op) {

		return new NullableFloatColumn((NonNullFloatColumn) subColumn.evaluate(op), subNonNulls(), null, 0, size);
	}
}
