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
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;
import static tech.bitey.dataframe.Pr.checkElementIndex;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.DoublePredicate;
import java.util.stream.DoubleStream;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSearch;
import tech.bitey.bufferstuff.BufferSort;
import tech.bitey.bufferstuff.BufferUtils;

final class NonNullDoubleColumn extends NonNullSingleBufferColumn<Double, DoubleColumn, NonNullDoubleColumn>
		implements DoubleColumn {

	static final Map<Integer, NonNullDoubleColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullDoubleColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullDoubleColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullDoubleColumn(EMPTY_BUFFER, 0, 0, c, false));
	}

	static NonNullDoubleColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	private final DoubleBuffer elements;

	NonNullDoubleColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);

		this.elements = buffer.asDoubleBuffer();
	}

	@Override
	NonNullDoubleColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullDoubleColumn(buffer, offset, size, characteristics, view);
	}

	double at(int index) {
		return elements.get(index);
	}

	@Override
	Double getNoOffset(int index) {
		return at(index);
	}

	int search(double value) {
		return BufferSearch.binarySearch(elements, offset, offset + size, value);
	}

	@Override
	int search(Double value, boolean first) {
		if (isSorted()) {
			int index = search(value);
			if (isDistinct() || index < 0)
				return index;
			else if (first)
				return BufferSearch.binaryFindFirst(elements, offset, index);
			else
				return BufferSearch.binaryFindLast(elements, offset + size, index);
		} else {
			double d = value;

			if (first) {
				for (int i = offset; i <= lastIndex(); i++)
					if (Double.compare(at(i), d) == 0)
						return i;
			} else {
				for (int i = lastIndex(); i >= offset; i--)
					if (Double.compare(at(i), d) == 0)
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
	NonNullDoubleColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public double getDouble(int index) {
		checkElementIndex(index, size);
		return at(index + offset);
	}

	@Override
	public ColumnType<Double> getType() {
		return ColumnType.DOUBLE;
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {
			long bits = Double.doubleToLongBits(at(i));
			result = 31 * result + (int) (bits ^ (bits >>> 32));
		}
		return result;
	}

	@Override
	NonNullDoubleColumn applyFilter0(BufferBitSet keep, int cardinality) {

		ByteBuffer buffer = allocate(cardinality);
		for (int i = offset; i <= lastIndex(); i++)
			if (keep.get(i - offset))
				buffer.putDouble(at(i));
		buffer.flip();

		return new NonNullDoubleColumn(buffer, 0, cardinality, characteristics, false);
	}

	@Override
	NonNullDoubleColumn select0(IntColumn indices) {

		ByteBuffer buffer = allocate(indices.size());
		for (int i = 0; i < indices.size(); i++)
			buffer.putDouble(at(indices.getInt(i) + offset));
		buffer.flip();

		return construct(buffer, 0, indices.size(), NONNULL, false);
	}

	@Override
	int compareValuesAt(NonNullDoubleColumn rhs, int l, int r) {
		return Double.compare(at(l + offset), rhs.at(r + rhs.offset));
	}

	@Override
	void intersectLeftSorted(NonNullDoubleColumn rhs, IntColumnBuilder indices, BufferBitSet keepRight) {

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
		return o instanceof Double;
	}

	@Override
	int elementSize() {
		return 8;
	}

	@Override
	public DoubleStream doubleStream() {
		return BufferUtils.stream(elements, offset, offset + size, characteristics);
	}

	@Override
	public DoubleColumn cleanDouble(DoublePredicate predicate) {

		return cleanDouble(predicate, new BufferBitSet());
	}

	DoubleColumn cleanDouble(DoublePredicate predicate, BufferBitSet nonNulls) {

		int cardinality = 0, i = 0;

		for (i = 0; i < size(); i++) {
			if (!predicate.test(at(i + offset))) {
				nonNulls.set(i);
				cardinality++;
			}
		}

		if (cardinality == size())
			return this;
		else {
			NonNullDoubleColumn filtered = applyFilter0(nonNulls, cardinality);
			return new NullableDoubleColumn(filtered, nonNulls, null, 0, size());
		}
	}
}
