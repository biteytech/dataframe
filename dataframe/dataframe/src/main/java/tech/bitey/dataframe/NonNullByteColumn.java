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
import static tech.bitey.dataframe.Pr.checkElementIndex;

import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

final class NonNullByteColumn extends ByteArrayColumn<Byte, ByteColumn, NonNullByteColumn> implements ByteColumn {

	static final Map<Integer, NonNullByteColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullByteColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullByteColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullByteColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullByteColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullByteColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, ByteArrayPacker.BYTE, offset, size, characteristics, view);
	}

	@Override
	NonNullByteColumn construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullByteColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullByteColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<Byte> getType() {
		return ColumnType.BYTE;
	}

	@Override
	public byte getByte(int index) {
		checkElementIndex(index, size);
		return at(index + offset);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Byte;
	}

	@Override
	public ByteColumn cleanByte(BytePredicate predicate) {

		return cleanByte(predicate, new BufferBitSet());
	}

	ByteColumn cleanByte(BytePredicate predicate, BufferBitSet nonNulls) {

		int cardinality = filterByte00(predicate, nonNulls, false);

		if (cardinality == size())
			return this;
		else {
			NonNullByteColumn filtered = applyFilter0(nonNulls, cardinality);
			return new NullableByteColumn(filtered, nonNulls, null, 0, size());
		}
	}

	@Override
	public ByteColumn filterByte(BytePredicate predicate, boolean keepNulls) {

		return filterByte0(predicate, new BufferBitSet());
	}

	ByteColumn filterByte0(BytePredicate predicate, BufferBitSet keep) {

		int cardinality = filterByte00(predicate, keep, true);

		if (cardinality == size())
			return this;
		else
			return applyFilter0(keep, cardinality);
	}

	private int filterByte00(BytePredicate predicate, BufferBitSet filter, boolean expected) {

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
	public ByteColumn evaluate(ByteUnaryOperator op) {

		final BigByteBuffer bb = allocate(size);

		for (int i = offset, j = 0; i <= lastIndex(); i++) {
			byte value = op.applyAsByte(at(i));
			bb.put(j++, value);
		}

		return new NonNullByteColumn(bb, 0, size, NONNULL_CHARACTERISTICS, false);
	}
}
