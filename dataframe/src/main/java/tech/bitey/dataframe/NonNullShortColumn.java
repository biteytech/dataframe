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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;
import static tech.bitey.dataframe.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSearch;
import tech.bitey.bufferstuff.BufferSort;
import tech.bitey.bufferstuff.BufferUtils;

final class NonNullShortColumn extends NonNullSingleBufferColumn<Short, ShortColumn, NonNullShortColumn>
		implements ShortColumn {

	static final Map<Integer, NonNullShortColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullShortColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullShortColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullShortColumn(EMPTY_BUFFER, 0, 0, c, false));
	}

	static NonNullShortColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	private final ShortBuffer elements;

	NonNullShortColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);

		this.elements = buffer.asShortBuffer();
	}

	@Override
	NonNullShortColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullShortColumn(buffer, offset, size, characteristics, view);
	}

	short at(int index) {
		return elements.get(index);
	}

	@Override
	Short getNoOffset(int index) {
		return at(index);
	}

	@Override
	public double min() {
		if (size == 0)
			return Double.NaN;
		else if (isSorted())
			return at(offset);

		short min = at(offset);

		for (int i = offset + 1; i <= lastIndex(); i++) {
			short x = at(i);
			if (x < min)
				min = x;
		}

		return min;
	}

	@Override
	public double max() {
		if (size == 0)
			return Double.NaN;
		else if (isSorted())
			return at(lastIndex());

		short max = at(offset);

		for (int i = offset + 1; i <= lastIndex(); i++) {
			short x = at(i);
			if (x > max)
				max = x;
		}

		return max;
	}

	@Override
	public double mean() {
		if (size == 0)
			return Double.NaN;

		long sum = 0;
		for (int i = offset; i <= lastIndex(); i++)
			sum += at(i);
		return sum / (double) size;
	}

	@Override
	public double stddev(boolean population) {
		if (size == 0 || (!population && size == 1))
			return Double.NaN;

		double μ = mean();

		double numer = 0;
		for (int i = offset; i <= lastIndex(); i++) {
			double d = at(i) - μ;
			numer += d * d;
		}

		return Math.sqrt(numer / (population ? size : size - 1));
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
	public Comparator<Short> comparator() {
		return Short::compareTo;
	}

	@Override
	public short getShort(int index) {
		checkElementIndex(index, size);
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

		ByteBuffer buffer = allocate(cardinality);
		for (int i = offset; i <= lastIndex(); i++)
			if (keep.get(i - offset))
				buffer.putShort(at(i));
		buffer.flip();

		return new NonNullShortColumn(buffer, 0, cardinality, characteristics, false);
	}

	@Override
	NonNullShortColumn select0(IntColumn indices) {

		ByteBuffer buffer = allocate(indices.size());
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
	IntColumn sortIndices(NonNullShortColumn distinct) {
		IntColumnBuilder indices = new IntColumnBuilder(NONNULL);
		indices.ensureCapacity(size());

		for (int i = offset; i <= lastIndex(); i++) {
			int index = distinct.search(at(i));
			indices.add(index);
		}

		return indices.build();
	}

	@Override
	int elementSize() {
		return 2;
	}
}