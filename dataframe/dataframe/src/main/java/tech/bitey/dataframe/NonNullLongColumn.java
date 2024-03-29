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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BIG_BUFFER;
import static tech.bitey.dataframe.LongArrayPacker.LONG;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;
import tech.bitey.bufferstuff.SmallLongBuffer;

final class NonNullLongColumn extends LongArrayColumn<Long, LongColumn, NonNullLongColumn> implements LongColumn {

	static final Map<Integer, NonNullLongColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullLongColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullLongColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullLongColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullLongColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullLongColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, LONG, offset, size, characteristics, view);
	}

	@Override
	NonNullLongColumn construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullLongColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullLongColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<Long> getType() {
		return ColumnType.LONG;
	}

	@Override
	public long getLong(int index) {
		Objects.checkIndex(index, size);
		return at(index + offset);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Long;
	}

	@Override
	public LongStream longStream() {
		return BufferUtils.stream(elements, offset, offset + size, characteristics);
	}

	@Override
	public LongColumn cleanLong(LongPredicate predicate) {

		return cleanLong(predicate, new BufferBitSet());
	}

	LongColumn cleanLong(LongPredicate predicate, BufferBitSet nonNulls) {

		int cardinality = filterLong00(predicate, nonNulls, false);

		if (cardinality == size())
			return this;
		else {
			NonNullLongColumn filtered = applyFilter0(nonNulls, cardinality);
			return new NullableLongColumn(filtered, nonNulls, null, 0, size());
		}
	}

	@Override
	public LongColumn filterLong(LongPredicate predicate, boolean keepNulls) {

		return filterLong0(predicate, new BufferBitSet());
	}

	LongColumn filterLong0(LongPredicate predicate, BufferBitSet keep) {

		int cardinality = filterLong00(predicate, keep, true);

		if (cardinality == size())
			return this;
		else
			return applyFilter0(keep, cardinality);
	}

	private int filterLong00(LongPredicate predicate, BufferBitSet filter, boolean expected) {

		int cardinality = 0;

		for (int i = size() - 1; i >= 0; i--) {
			if (predicate.test(at(i + offset)) == expected) {
				filter.set(i);
				cardinality++;
			}
		}

		return cardinality;
	}

	@Override
	public LongColumn evaluate(LongUnaryOperator op) {

		final BigByteBuffer bb = allocate(size);
		final SmallLongBuffer buf = bb.asLongBuffer();

		for (int i = offset; i <= lastIndex(); i++) {
			long value = op.applyAsLong(at(i));
			buf.put(value);
		}

		return new NonNullLongColumn(bb, 0, size, NONNULL_CHARACTERISTICS, false);
	}
}
