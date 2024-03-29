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

final class NullableShortColumn extends NullableColumn<Short, ShortColumn, NonNullShortColumn, NullableShortColumn>
		implements ShortColumn {

	NullableShortColumn(NonNullColumn<Short, ShortColumn, NonNullShortColumn> column, BufferBitSet nonNulls,
			INullCounts nullCounts, int offset, int size) {
		super((NonNullShortColumn) column, nonNulls, nullCounts, offset, size);
	}

	@Override
	public short getShort(int index) {
		checkGetPrimitive(index);
		return column.getShort(nonNullIndex(index + offset));
	}

	@Override
	public ShortColumn cleanShort(ShortPredicate predicate) {

		BufferBitSet cleanNonNulls = new BufferBitSet();
		ShortColumn cleaned = subColumn.cleanShort(predicate, cleanNonNulls);

		return clean(cleaned, cleanNonNulls);
	}

	@Override
	public ShortColumn filterShort(ShortPredicate predicate, boolean keepNulls) {

		BufferBitSet keep = new BufferBitSet();
		var filtered = subColumn.filterShort0(predicate, keep);

		return filter(filtered, keep, keepNulls);
	}

	@Override
	public ShortColumn evaluate(ShortUnaryOperator op) {

		return new NullableShortColumn((NonNullShortColumn) subColumn.evaluate(op), subNonNulls(), null, 0, size);
	}
}
