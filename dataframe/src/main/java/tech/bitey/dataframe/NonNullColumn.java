/*
 * Copyright 2020 biteytech@protonmail.com
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
import static tech.bitey.dataframe.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.DfPreconditions.checkPositionIndex;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.Buffer;
import java.nio.channels.ReadableByteChannel;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import tech.bitey.bufferstuff.BufferBitSet;

@SuppressWarnings({ "unchecked", "rawtypes" })
abstract class NonNullColumn<E, I extends Column<E>, C extends NonNullColumn<E, I, C>> extends AbstractColumn<E, I, C> {
	static final int NONNULL_CHARACTERISTICS = BASE_CHARACTERISTICS | NONNULL;

	final int characteristics;
	final boolean view;

	NonNullColumn(int offset, int size, int characteristics, boolean view) {
		super(offset, size);

		this.characteristics = NONNULL_CHARACTERISTICS | characteristics;
		this.view = view;
	}

	@Override
	public int characteristics() {
		return characteristics;
	}

	@Override
	public boolean isNullNoOffset(int index) {
		return false;
	}

	abstract int search(E value, boolean first);

	abstract C withCharacteristics(int characteristics);

	abstract boolean checkSorted();

	abstract boolean checkDistinct();

	abstract C toSorted0();

	abstract C toDistinct0(boolean sort);

	abstract C slice();

	abstract C readFrom(ReadableByteChannel channel) throws IOException;

	abstract IntColumn sortIndices(C distinct);

	@Override
	public C toHeap() {
		if (isSorted())
			return withCharacteristics(NONNULL_CHARACTERISTICS);
		else {
			return (C) this;
		}
	}

	@Override
	public C toSorted() {
		if (isDistinct())
			return withCharacteristics(characteristics & ~DISTINCT);
		else if (isSorted()) {
			return (C) this;
		} else {
			if (checkSorted())
				return withCharacteristics(characteristics | SORTED);
			else
				return toSorted0();
		}
	}

	@Override
	public C toDistinct() {
		if (isDistinct())
			return (C) this;
		else if (isSorted())
			return sortedToDistinct();
		else {
			if (checkSorted())
				return sortedToDistinct();
			else
				return toDistinct0(true);
		}
	}

	private C sortedToDistinct() {
		if (checkDistinct())
			return withCharacteristics(characteristics | SORTED | DISTINCT);
		else
			return toDistinct0(false);
	}

	/*
	 * Object[0] -> distinct column Object[1] -> sorted indices
	 */
	Object[] toDistinctWithIndices() {

		C distinct = toDistinct();
		DfPreconditions.checkState(distinct.size() == size(), "elements must already be distinct elements");

		IntColumn indices = sortIndices(distinct);

		return new Object[] { distinct, indices };
	}

	/*
	 * @param fromIndex - inclusive
	 * 
	 * @param toIndex - inclusive
	 */
	abstract int hashCode(int fromIndex, int toIndex);

	abstract boolean equals0(C rhs, int lStart, int rStart, int length);

	@Override
	boolean equals0(C rhs) {
		return equals0(rhs, offset, rhs.offset, size);
	}

	abstract C appendNonNull(C tail);

	@Override
	I append0(Column<E> tail) {

		if (!tail.isNonnull()) {
			NullableColumn rhs = (NullableColumn) tail;
			C lhs = (C) this;
			return (I) rhs.prependNonNull(lhs);
		} else {
			checkArgument(characteristics == tail.characteristics(), "both columns must have the same characteristics");

			return (I) appendNonNull((C) tail);
		}
	}

	abstract int compareValuesAt(C rhs, int l, int r);

	@Override
	int intersectBothSorted(C rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {

		int l = 0;
		int r = 0;
		int cardinality = 0;

		OUTER_LOOP: while (l < this.size() && r < rhs.size()) {

			for (int direction; (direction = compareValuesAt(rhs, l, r)) != 0;) {

				if (direction < 0) {
					if (++l == this.size())
						break OUTER_LOOP;
				} else {
					if (++r == rhs.size())
						break OUTER_LOOP;
				}
			}

			keepLeft.set(l++);
			keepRight.set(r++);
			cardinality++;
		}

		return cardinality;
	}

	abstract void intersectLeftSorted(C rhs, IntColumnBuilder indices, BufferBitSet keepRight);

	@Override
	IntColumn intersectLeftSorted(I rhs, BufferBitSet keepRight) {
		IntColumnBuilder indices = IntColumn.builder();
		if (rhs.isNonnull())
			intersectLeftSorted((C) rhs, indices, keepRight);
		else
			((NullableColumn) rhs).intersectRightSorted(this, indices, keepRight);
		return indices.build();
	}

	@Override
	public int hashCode() {
		if (isEmpty())
			return 1;
		else
			return hashCode(offset, lastIndex());
	}

	@Override
	public Spliterator<E> spliterator() {
		return Spliterators.spliterator(this, characteristics);
	}

	@Override
	public int indexOf(Object o) {
		if (!checkType(o))
			return -1;

		int index = search((E) o, true);
		return index < 0 ? -1 : index - offset;
	}

	@Override
	public int lastIndexOf(Object o) {
		if (!checkType(o))
			return -1;

		int index = search((E) o, false);
		return index < 0 ? -1 : index - offset;
	}

	int findIndexOrInsertionPoint(E value) {
		int index = search(value, true);

		if (index < 0) {
			// index is (-(insertion point) - 1)
			index = -(index + 1);
		}

		return index - offset;
	}

	@Override
	public boolean contains(Object o) {
		return o == null ? false : indexOf(o) != -1;
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

				return getNoOffset(index++);
			}

			@Override
			public boolean hasPrevious() {
				return index > offset;
			}

			@Override
			public E previous() {
				if (!hasPrevious())
					throw new NoSuchElementException("called previous when hasPrevious is false");

				return getNoOffset(--index);
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

	/*------------------------------------------------------------
	 *                ImmutableNavigableSet methods
	 *------------------------------------------------------------*/
	void verifyDistinct() {
		if (!isDistinct())
			throw new UnsupportedOperationException("not a unique index");
	}

	@Override
	public E lower(E value) {
		if (value == null)
			return null;
		verifyDistinct();

		int idx = lowerIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}

	@Override
	public E higher(E value) {
		if (value == null)
			return null;
		verifyDistinct();

		int idx = higherIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}

	@Override
	public E floor(E value) {
		if (value == null)
			return null;
		verifyDistinct();

		int idx = floorIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}

	@Override
	public E ceiling(E value) {
		if (value == null)
			return null;
		verifyDistinct();

		int idx = ceilingIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}

	int lowerIndex(E value) {
		int idx = search(value, true);

		if (idx < 0)
			idx = -(idx + 1);

		if (idx <= offset)
			return -1;
		else if (idx > lastIndex())
			return lastIndex();
		else
			return idx - 1;
	}

	int higherIndex(E value) {
		int idx = search(value, true);

		if (idx < 0)
			if (isEmpty())
				return -1;
			else
				idx = -(idx + 1) - 1;

		if (idx < offset)
			return offset;
		else if (idx >= lastIndex())
			return -1;
		else
			return idx + 1;
	}

	int floorIndex(E value) {
		int idx = search(value, true);

		if (idx >= 0)
			return idx;
		else
			idx = -(idx + 1);

		if (idx <= offset)
			return -1;
		else if (idx > lastIndex())
			return lastIndex();
		else
			return idx - 1;
	}

	int ceilingIndex(E value) {
		int idx = search(value, true);

		if (idx >= 0)
			return idx;
		else if (isEmpty())
			return -1;
		else
			idx = -(idx + 1) - 1;

		if (idx < offset)
			return offset;
		else if (idx >= lastIndex())
			return -1;
		else
			return idx + 1;
	}

	/*------------------------------------------------------------
	 *                subColumn methods
	 *------------------------------------------------------------*/
	@Override
	public C subColumnByValue(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		verifyDistinct();

		int direction = comparator().compare(fromElement, toElement);

		checkArgument(direction <= 0, "from cannot be greater than to");

		if (isEmpty() || (direction == 0 && !(fromInclusive && toInclusive)))
			return empty();

		int fk = fromInclusive ? ceilingIndex(fromElement) : higherIndex(fromElement);
		if (fk == -1)
			return empty();

		int tk = toInclusive ? floorIndex(toElement) : lowerIndex(toElement);
		if (tk == -1)
			return empty();

		return subColumn(fk - offset, tk + 1 - offset);
	}

	@Override
	public C subColumnByValue(E fromElement, E toElement) {
		return subColumnByValue(fromElement, true, toElement, true);
	}

	@Override
	public C head(E toElement, boolean inclusive) {

		if (isEmpty())
			return empty();

		final E from = first();

		if (comparator().compare(toElement, from) < 0)
			return empty();

		return subColumnByValue(from, true, toElement, inclusive);
	}

	@Override
	public C head(E toElement) {
		return head(toElement, false);
	}

	@Override
	public C tail(E fromElement, boolean inclusive) {

		if (isEmpty())
			return empty();

		final E to = last();

		if (comparator().compare(fromElement, to) > 0)
			return empty();

		return subColumnByValue(fromElement, inclusive, to, true);
	}

	@Override
	public C tail(E fromElement) {
		return tail(fromElement, true);
	}

	void validateBuffer(Buffer buffer) {
		checkArgument(buffer.position() == 0, "buffer position must be zero");
	}

	@Override
	public NonNullBooleanColumn toBooleanColumn(Predicate<E> predicate) {

		BooleanColumnBuilder builder = new BooleanColumnBuilder();
		builder.ensureCapacity(size);

		for (E element : this) {
			boolean value = predicate.test(element);
			builder.add(value);
		}

		return (NonNullBooleanColumn) builder.build();
	}

	@Override
	public NonNullDateColumn toDateColumn(Function<E, LocalDate> function) {

		DateColumnBuilder builder = new DateColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			LocalDate value = function.apply(element);
			builder.add(value);
		}

		return (NonNullDateColumn) builder.build();
	}

	@Override
	public NonNullDateTimeColumn toDateTimeColumn(Function<E, LocalDateTime> function) {

		DateTimeColumnBuilder builder = new DateTimeColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			LocalDateTime value = function.apply(element);
			builder.add(value);
		}

		return (NonNullDateTimeColumn) builder.build();
	}

	@Override
	public NonNullDoubleColumn toDoubleColumn(ToDoubleFunction<E> function) {

		DoubleColumnBuilder builder = new DoubleColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			double value = function.applyAsDouble(element);
			builder.add(value);
		}

		return (NonNullDoubleColumn) builder.build();
	}

	@Override
	public NonNullFloatColumn toFloatColumn(ToFloatFunction<E> function) {

		FloatColumnBuilder builder = new FloatColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			float value = function.applyAsFloat(element);
			builder.add(value);
		}

		return (NonNullFloatColumn) builder.build();
	}

	@Override
	public NonNullIntColumn toIntColumn(ToIntFunction<E> function) {

		IntColumnBuilder builder = new IntColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			int value = function.applyAsInt(element);
			builder.add(value);
		}

		return (NonNullIntColumn) builder.build();
	}

	@Override
	public NonNullLongColumn toLongColumn(ToLongFunction<E> function) {

		LongColumnBuilder builder = new LongColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			long value = function.applyAsLong(element);
			builder.add(value);
		}

		return (NonNullLongColumn) builder.build();
	}

	@Override
	public NonNullShortColumn toShortColumn(ToShortFunction<E> function) {

		ShortColumnBuilder builder = new ShortColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			short value = function.applyAsShort(element);
			builder.add(value);
		}

		return (NonNullShortColumn) builder.build();
	}

	@Override
	public NonNullByteColumn toByteColumn(ToByteFunction<E> function) {

		ByteColumnBuilder builder = new ByteColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			byte value = function.applyAsByte(element);
			builder.add(value);
		}

		return (NonNullByteColumn) builder.build();
	}

	@Override
	public NonNullDecimalColumn toDecimalColumn(Function<E, BigDecimal> function) {

		DecimalColumnBuilder builder = new DecimalColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			BigDecimal value = function.apply(element);
			builder.add(value);
		}

		return (NonNullDecimalColumn) builder.build();
	}

	@Override
	public NonNullStringColumn toStringColumn(Function<E, String> function) {

		StringColumnBuilder builder = new StringColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			String value = function.apply(element);
			builder.add(value);
		}

		return (NonNullStringColumn) builder.build();
	}

	@Override
	public NonNullUuidColumn toUuidColumn(Function<E, UUID> function) {

		UuidColumnBuilder builder = new UuidColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(size);

		for (E element : this) {
			UUID value = function.apply(element);
			builder.add(value);
		}

		return (NonNullUuidColumn) builder.build();
	}
}
