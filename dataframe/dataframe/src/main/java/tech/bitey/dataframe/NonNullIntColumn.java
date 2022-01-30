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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.dataframe.IntArrayPacker.INTEGER;
import static tech.bitey.dataframe.Pr.checkElementIndex;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;

final class NonNullIntColumn extends IntArrayColumn<Integer, IntColumn, NonNullIntColumn> implements IntColumn {

	static final Map<Integer, NonNullIntColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullIntColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullIntColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullIntColumn(EMPTY_BUFFER, 0, 0, c, false));
	}

	static NonNullIntColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullIntColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, INTEGER, offset, size, characteristics, view);
	}

	@Override
	NonNullIntColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullIntColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullIntColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<Integer> getType() {
		return ColumnType.INT;
	}

	@Override
	public int getInt(int index) {
		checkElementIndex(index, size);
		return at(index + offset);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Integer;
	}

	@Override
	public IntStream intStream() {
		return BufferUtils.stream(elements, offset, offset + size, characteristics);
	}

	@Override
	public IntColumn cleanInt(IntPredicate predicate) {

		return cleanInt(predicate, new BufferBitSet());
	}

	IntColumn cleanInt(IntPredicate predicate, BufferBitSet nonNulls) {

		int cardinality = filterInt00(predicate, nonNulls, false);

		if (cardinality == size())
			return this;
		else {
			NonNullIntColumn filtered = applyFilter0(nonNulls, cardinality);
			return new NullableIntColumn(filtered, nonNulls, null, 0, size());
		}
	}

	@Override
	public IntColumn filterInt(IntPredicate predicate, boolean keepNulls) {

		return filterInt0(predicate, new BufferBitSet());
	}

	IntColumn filterInt0(IntPredicate predicate, BufferBitSet keep) {

		int cardinality = filterInt00(predicate, keep, true);

		if (cardinality == size())
			return this;
		else
			return applyFilter0(keep, cardinality);
	}

	private int filterInt00(IntPredicate predicate, BufferBitSet filter, boolean expected) {

		int cardinality = 0;

		for (int i = size() - 1; i >= 0; i--) {
			if (predicate.test(at(i + offset)) == expected) {
				filter.set(i);
				cardinality++;
			}
		}

		return cardinality;
	}
}
