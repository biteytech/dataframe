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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static tech.bitey.bufferstuff.BufferBitSet.EMPTY_BITSET;
import static tech.bitey.dataframe.Pr.checkElementIndex;
import static tech.bitey.dataframe.Pr.checkPositionIndex;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.WritableByteChannel;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.EnumMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import tech.bitey.bufferstuff.BufferBitSet;

@SuppressWarnings({ "unchecked", "rawtypes" })
abstract class NullableColumn<E extends Comparable<? super E>, I extends Column<E>, C extends NonNullColumn<E, I, C>, N extends NullableColumn<E, I, C, N>>
		extends AbstractColumn<E, I, N> {

	static final Map<ColumnTypeCode, NullableColumn> EMPTY_MAP = new EnumMap<>(ColumnTypeCode.class);

	final C column;
	final C subColumn;
	final BufferBitSet nonNulls;
	final INullCounts nullCounts;

	NullableColumn(C column, BufferBitSet nonNulls, INullCounts nullCounts, int offset, int size) {
		super(offset, size);

		if (column.view)
			column = column.slice();

		this.column = column;
		this.nonNulls = nonNulls;
		this.nullCounts = nullCounts == null ? INullCounts.of(nonNulls, size) : nullCounts;

		final int firstNonNullIndex = firstNonNullIndex();
		if (firstNonNullIndex == -1)
			this.subColumn = column.empty();
		else
			this.subColumn = column.subColumn(firstNonNullIndex, lastNonNullIndex() + 1);
	}

	@Override
	public int characteristics() {
		return BASE_CHARACTERISTICS;
	}

	@Override
	public N toHeap() {
		return (N) this;
	}

	@Override
	public N toSorted() {
		throw new UnsupportedOperationException("columns with null values cannot be sorted");
	}

	@Override
	public N toDistinct() {
		throw new UnsupportedOperationException("columns with null values cannot be sorted");
	}

	@Override
	E getNoOffset(int index) {
		if (nonNulls.get(index))
			return column.getNoOffset(nonNullIndex(index));
		else
			return null;
	}

	@Override
	boolean isNullNoOffset(int index) {
		return !nonNulls.get(index);
	}

	private int firstNonNullIndex() {
		int index = nonNulls.nextSetBit(offset);
		return index == -1 || index > lastIndex() ? -1 : nonNullIndex(index);
	}

	private int lastNonNullIndex() {
		int index = nonNulls.previousSetBit(lastIndex());
		return index < offset ? -1 : nonNullIndex(index);
	}

	int nonNullIndex(int index) {
		return nullCounts.nonNullIndex(index);
	}

	private int nullIndex(int index) {

		int nullIndex = -1;
		for (int i = 0; i <= index; i++)
			nullIndex = nonNulls.nextSetBit(nullIndex + 1);

		return nullIndex;
	}

	void checkGetPrimitive(int index) {
		checkElementIndex(index, size);

		if (isNullNoOffset(index + offset))
			throw new NullPointerException();
	}

	private int indexOf0(Object o, boolean first) {
		if ((o != null && !checkType(o)) || isEmpty())
			return -1;
		else if (o == null) {
			if (first) {
				int index = nonNulls.nextClearBit(offset);
				return index > lastIndex() ? -1 : index - offset;
			} else {
				int index = nonNulls.previousClearBit(lastIndex());
				return index < offset ? -1 : index - offset;
			}
		}

		int index = subColumn.search((E) o, first);
		index = nullIndex(index);

		return index < offset || index > lastIndex() ? -1 : index - offset;
	}

	@Override
	public int indexOf(Object o) {
		return indexOf0(o, true);
	}

	@Override
	public int lastIndexOf(Object o) {
		return indexOf0(o, false);
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o) != -1;
	}

	@Override
	public ListIterator<E> listIterator(final int idx) {

		checkPositionIndex(idx, size);

		return new ImmutableListIterator<E>() {

			int index = idx + offset;

			@Override
			public boolean hasNext() {
				return index <= lastIndex();
			}

			@Override
			public E next() {
				if (!hasNext())
					throw new NoSuchElementException("called next when hasNext is false");

				if (nonNulls.get(index))
					return column.get(nonNullIndex(index++));
				else {
					index++;
					return null;
				}
			}

			@Override
			public boolean hasPrevious() {
				return index > offset;
			}

			@Override
			public E previous() {
				if (!hasPrevious())
					throw new NoSuchElementException("called previous when hasPrevious is false");

				if (nonNulls.get(--index))
					return column.get(nonNullIndex(index));
				else {
					return null;
				}
			}

			@Override
			public int nextIndex() {
				return index - offset;
			}

			@Override
			public int previousIndex() {
				return index - offset - 1;
			}
		};
	}

	@Override
	boolean equals0(NullableColumn rhs) {

		// check that values and nulls are in the same place
		int count = 0;
		for (int i = offset, j = rhs.offset; i <= lastIndex(); i++, j++) {
			if (nonNulls.get(i) != rhs.nonNulls.get(j))
				return false;
			count += nonNulls.get(i) ? 1 : 0;
		}

		if (count > 0)
			return column.equals0((C) rhs.column, firstNonNullIndex(), rhs.firstNonNullIndex(), count);
		else
			return true; // all nulls
	}

	@Override
	public int hashCode() {
		if (isEmpty())
			return 1;

		// hash value positions
		int result = 1;
		for (int i = offset; i <= lastIndex(); i++)
			result = 31 * result + (nonNulls.get(i) ? 1231 : 1237);

		// hash actual values
		return 31 * result + subColumn.hashCode();
	}

	N construct(C column, BufferBitSet nonNulls, int size) {
		return (N) getType().nullableConstructor().create(column, nonNulls, null, 0, size);
	}

	@Override
	N subColumn0(int fromIndex, int toIndex) {
		return (N) getType().nullableConstructor().create(column, nonNulls, nullCounts, fromIndex + offset,
				toIndex - fromIndex);
	}

	@Override
	Column<E> applyFilter0(BufferBitSet keep, int cardinality) {

		if (keep.equals(nonNulls))
			return column;

		BufferBitSet filteredNonNulls = new BufferBitSet();
		BufferBitSet keepNonNulls = new BufferBitSet();

		int nullCount = 0;
		for (int i = offset, j = 0; i <= lastIndex(); i++) {
			if (keep.get(i - offset)) {
				if (nonNulls.get(i)) {
					filteredNonNulls.set(j);
					keepNonNulls.set(nonNullIndex(i));
				} else
					nullCount++;
				j++;
			}
		}

		C column = (C) this.column.applyFilter(keepNonNulls, keepNonNulls.cardinality());

		if (nullCount == 0)
			return column;
		else
			return construct(column, filteredNonNulls, column.size() + nullCount);
	}

	@Override
	public N clean(Predicate<E> predicate) {

		BufferBitSet cleanNonNulls = new BufferBitSet();
		I cleaned = subColumn.clean(predicate, cleanNonNulls);

		if (cleaned == subColumn)
			return (N) this;
		else if (cleaned.isEmpty())
			return construct((C) cleaned, cleanNonNulls, size);

		N nullableCleaned = (N) cleaned;

		BufferBitSet nonNulls = new BufferBitSet();
		for (int i = offset, j = 0; i <= lastIndex(); i++)
			if (this.nonNulls.get(i) && nullableCleaned.nonNulls.get(j++))
				nonNulls.set(i - offset);

		return construct(nullableCleaned.column, nonNulls, size);
	}

	@Override
	Column<E> select0(IntColumn indices) {

		BufferBitSet decodedNonNulls = new BufferBitSet();
		int cardinality = 0;
		for (int i = 0; i < indices.size(); i++) {
			if (nonNulls.get(indices.getInt(i) + offset)) {
				decodedNonNulls.set(i);
				cardinality++;
			}
		}

		IntColumnBuilder nonNullIndices = IntColumn.builder().ensureCapacity(cardinality);
		for (int i = 0; i < indices.size(); i++) {
			int index = indices.getInt(i) + offset;
			if (nonNulls.get(index))
				nonNullIndices.add(nonNullIndex(index));
		}

		C column = (C) this.column.select(nonNullIndices.build());

		if (cardinality == indices.size())
			return column;
		else
			return construct(column, decodedNonNulls, indices.size());
	}

	I prependNonNull(C head) {

		BufferBitSet nonNulls = subNonNulls();
		nonNulls = nonNulls.shiftRight(head.size());
		nonNulls.set(0, head.size());

		final C column;
		if (this.subColumn.isEmpty())
			column = head;
		else
			column = head.appendNonNull(this.subColumn);

		return (I) construct(column, nonNulls, head.size() + this.size());
	}

	@Override
	I append0(Column<E> tail) {

		final int size = this.size() + tail.size();

		if (!tail.isNonnull()) {
			// append nullable column
			N rhs = (N) tail;

			C column = (C) this.subColumn.append(rhs.subColumn);

			BufferBitSet nonNulls = this.subNonNulls();
			BufferBitSet bothNonNulls = rhs.subNonNulls();

			bothNonNulls = bothNonNulls.shiftRight(size());
			bothNonNulls.or(nonNulls);

			return (I) construct(column, bothNonNulls, size);
		} else {
			// append non-null column
			C rhs = (C) tail;

			C column = (C) this.subColumn.append(rhs);

			BufferBitSet nonNulls = this.subNonNulls();
			nonNulls.set(this.size(), size);

			return (I) construct(column, nonNulls, size);
		}
	}

	@Override
	public N copy() {
		C column = (C) subColumn.copy();

		return construct(column, subNonNulls(), size);
	}

	@Override
	int intersectBothSorted(N rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectBothSorted");
	}

	@Override
	IntColumn intersectLeftSorted(I rhs, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectLeftSorted");
	}

	void intersectRightSorted(C rhs, IntColumnBuilder indices, BufferBitSet keepLeft) {

		for (int i = offset; i <= lastIndex(); i++) {

			if (!nonNulls.get(i))
				continue;

			int rightIndex = rhs.search(column.getNoOffset(nonNullIndex(i)), true);
			if (rightIndex >= rhs.offset && rightIndex <= rhs.lastIndex()) {

				indices.add(rightIndex - rhs.offset);
				keepLeft.set(i - offset);
			}
		}
	}

	@Override
	void writeTo(WritableByteChannel channel) throws IOException {
		writeInt(channel, BIG_ENDIAN, size);
		nonNulls.writeTo(channel, offset, offset + size);
		subColumn.writeTo(channel);
	}

	/*------------------------------------------------------------
	 *  Type Conversion Methods
	 *------------------------------------------------------------*/
	@Override
	public BooleanColumn toBooleanColumn(Predicate<E> predicate) {
		return new NullableBooleanColumn(subColumn.toBooleanColumn(predicate), subNonNulls(), null, 0, size);
	}

	@Override
	public DateColumn toDateColumn(Function<E, LocalDate> function) {
		return new NullableDateColumn(subColumn.toDateColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public DateTimeColumn toDateTimeColumn(Function<E, LocalDateTime> function) {
		return new NullableDateTimeColumn(subColumn.toDateTimeColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public DoubleColumn toDoubleColumn(ToDoubleFunction<E> function) {
		return new NullableDoubleColumn(subColumn.toDoubleColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public FloatColumn toFloatColumn(ToFloatFunction<E> function) {
		return new NullableFloatColumn(subColumn.toFloatColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public IntColumn toIntColumn(ToIntFunction<E> function) {
		return new NullableIntColumn(subColumn.toIntColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public LongColumn toLongColumn(ToLongFunction<E> function) {
		return new NullableLongColumn(subColumn.toLongColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public ShortColumn toShortColumn(ToShortFunction<E> function) {
		return new NullableShortColumn(subColumn.toShortColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public ByteColumn toByteColumn(ToByteFunction<E> function) {
		return new NullableByteColumn(subColumn.toByteColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public DecimalColumn toDecimalColumn(Function<E, BigDecimal> function) {
		return new NullableDecimalColumn(subColumn.toDecimalColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public StringColumn toStringColumn(Function<E, String> function) {
		return new NullableStringColumn(subColumn.toStringColumn(function), subNonNulls(), null, 0, size);
	}

	@Override
	public UuidColumn toUuidColumn(Function<E, UUID> function) {
		return new NullableUuidColumn(subColumn.toUuidColumn(function), subNonNulls(), null, 0, size);
	}

	BufferBitSet subNonNulls() {
		return nonNulls.get(offset, offset + size);
	}

	// does not implement navigableset methods

	@Override
	public E lower(E e) {
		throw new UnsupportedOperationException("lower");
	}

	@Override
	public E floor(E e) {
		throw new UnsupportedOperationException("floor");
	}

	@Override
	public E ceiling(E e) {
		throw new UnsupportedOperationException("ceiling");
	}

	@Override
	public E higher(E e) {
		throw new UnsupportedOperationException("higher");
	}

	// does not implement subColumn-by-element methods

	@Override
	public N subColumnByValue(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		throw new UnsupportedOperationException("subColumn");
	}

	@Override
	public N subColumnByValue(E fromElement, E toElement) {
		throw new UnsupportedOperationException("subColumn");
	}

	@Override
	public N head(E toElement, boolean inclusive) {
		throw new UnsupportedOperationException("head");
	}

	@Override
	public N head(E toElement) {
		throw new UnsupportedOperationException("head");
	}

	@Override
	public N tail(E fromElement, boolean inclusive) {
		throw new UnsupportedOperationException("tail");
	}

	@Override
	public N tail(E fromElement) {
		throw new UnsupportedOperationException("tail");
	}

	@Override
	public NavigableSet<E> asSet() {
		throw new UnsupportedOperationException("asSet");
	}

	@Override
	public ColumnType getType() {
		return column.getType();
	}

	@Override
	boolean checkType(Object o) {
		return column.checkType(o);
	}

	static {
		for (ColumnTypeCode typeCode : ColumnTypeCode.values()) {

			if (typeCode == ColumnTypeCode.NS)
				continue;

			ColumnType type = typeCode.getType();
			NullableColumn empty = type.nullableConstructor().create((NonNullColumn) type.builder().build(),
					EMPTY_BITSET, null, 0, 0);
			EMPTY_MAP.put(typeCode, empty);
		}
	}

	@Override
	N empty() {
		return (N) EMPTY_MAP.get(getType().getCode());
	}
}
