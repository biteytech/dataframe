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
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BIG_BUFFER;
import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSearch;
import tech.bitey.bufferstuff.BufferSort;
import tech.bitey.bufferstuff.BufferUtils;
import tech.bitey.bufferstuff.SmallShortBuffer;

final class NonNullShortColumn extends NonNullSingleBufferColumn<Short, ShortColumn, NonNullShortColumn>
		implements ShortColumn {

	static final Map<Integer, NonNullShortColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullShortColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullShortColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullShortColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullShortColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	private final SmallShortBuffer elements;

	NonNullShortColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);

		this.elements = buffer.asShortBuffer();
	}

	@Override
	NonNullShortColumn construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullShortColumn(buffer, offset, size, characteristics, view);
	}

	short at(int index) {
		return elements.get(index);
	}

	@Override
	Short getNoOffset(int index) {
		return at(index);
	}

	int search(short value) {
		return BufferSearch.binarySearch(elements, offset, offset + size, value);
	}

	@Override
	int search(Short value, boolean first) {
		if (isSorted()) {
			int index = search(value);
			if (isDistinct() || index < 0)
				return index;
			else if (first)
				return BufferSearch.binaryFindFirst(elements, offset, index);
			else
				return BufferSearch.binaryFindLast(elements, offset + size, index);
		} else {
			short d = value;

			if (first) {
				for (int i = offset; i <= lastIndex(); i++)
					if (Short.compare(at(i), d) == 0)
						return i;
			} else {
				for (int i = lastIndex(); i >= offset; i--)
					if (Short.compare(at(i), d) == 0)
						return i;
			}

			return -1;
		}
	}

	@Override
	void sort() {
		BufferSort.sort(elements, offset, offset + size);
	}

	@Override
	int deduplicate() {
		return BufferUtils.deduplicate(elements, offset, offset + size);
	}

	@Override
	boolean checkSorted() {
		return BufferUtils.isSorted(elements, offset, offset + size);
	}

	@Override
	boolean checkDistinct() {
		return isSortedAndDistinct(elements, offset, offset + size);
	}

	@Override
	NonNullShortColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public short getShort(int index) {
		Objects.checkIndex(index, size);
		return at(index + offset);
	}

	@Override
	public ColumnType<Short> getType() {
		return ColumnType.SHORT;
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {
			result = 31 * result + at(i);
		}
		return result;
	}

	@Override
	NonNullShortColumn applyFilter0(BufferBitSet keep, int cardinality) {

		BigByteBuffer buffer = allocate(cardinality);
		for (int i = offset; i <= lastIndex(); i++)
			if (keep.get(i - offset))
				buffer.putShort(at(i));
		buffer.flip();

		return new NonNullShortColumn(buffer, 0, cardinality, characteristics, false);
	}

	@Override
	NonNullShortColumn select0(IntColumn indices) {

		BigByteBuffer buffer = allocate(indices.size());
		for (int i = 0; i < indices.size(); i++)
			buffer.putShort(at(indices.getInt(i) + offset));
		buffer.flip();

		return construct(buffer, 0, indices.size(), NONNULL, false);
	}

	@Override
	int compareValuesAt(NonNullShortColumn rhs, int l, int r) {
		return Short.compare(at(l + offset), rhs.at(r + rhs.offset));
	}

	@Override
	void intersectLeftSorted(NonNullShortColumn rhs, IntColumnBuilder indices, BufferBitSet keepRight) {

		for (int i = rhs.offset; i <= rhs.lastIndex(); i++) {

			int leftIndex = search(rhs.at(i));
			if (leftIndex >= offset && leftIndex <= lastIndex()) {

				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Short;
	}

	@Override
	int elementSize() {
		return 2;
	}

	@Override
	public ShortColumn cleanShort(ShortPredicate predicate) {

		return cleanShort(predicate, new BufferBitSet());
	}

	ShortColumn cleanShort(ShortPredicate predicate, BufferBitSet nonNulls) {

		int cardinality = filterShort00(predicate, nonNulls, false);

		if (cardinality == size())
			return this;
		else {
			NonNullShortColumn filtered = applyFilter0(nonNulls, cardinality);
			return new NullableShortColumn(filtered, nonNulls, null, 0, size());
		}
	}

	@Override
	public ShortColumn filterShort(ShortPredicate predicate, boolean keepNulls) {

		return filterShort0(predicate, new BufferBitSet());
	}

	ShortColumn filterShort0(ShortPredicate predicate, BufferBitSet keep) {

		int cardinality = filterShort00(predicate, keep, true);

		if (cardinality == size())
			return this;
		else
			return applyFilter0(keep, cardinality);
	}

	private int filterShort00(ShortPredicate predicate, BufferBitSet filter, boolean expected) {

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
	public ShortColumn evaluate(ShortUnaryOperator op) {

		final BigByteBuffer bb = allocate(size);
		final SmallShortBuffer buf = bb.asShortBuffer();

		for (int i = offset; i <= lastIndex(); i++) {
			short value = op.applyAsShort(at(i));
			buf.put(value);
		}

		return new NonNullShortColumn(bb, 0, size, NONNULL_CHARACTERISTICS, false);
	}
}
