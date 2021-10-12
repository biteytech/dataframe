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
import static tech.bitey.bufferstuff.BufferUtils.writeFully;
import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;
import static tech.bitey.dataframe.Pr.checkPositionIndex;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import tech.bitey.bufferstuff.BufferBitSet;

public class NormalStringColumnImpl extends AbstractColumn<String, NormalStringColumn, NormalStringColumnImpl>
		implements NormalStringColumn {

	static final NormalStringColumnImpl EMPTY = new NormalStringColumnImpl(
			NonNullByteColumn.empty(NONNULL_CHARACTERISTICS), Map.of(), 0, 0);

	final ByteColumn bytes;
	final Map<String, Integer> indices;
	final String[] values;

	NormalStringColumnImpl(ByteColumn bytes, Map<String, Integer> indices, int offset, int size) {
		super(offset, size);

		this.bytes = bytes;
		this.indices = indices;

		this.values = new String[indices.size()];
		for (var e : indices.entrySet())
			values[e.getValue()] = e.getKey();
	}

	@Override
	NormalStringColumnImpl empty() {
		return EMPTY;
	}

	@Override
	public int characteristics() {
		return bytes.characteristics();
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.NSTRING;
	}

	private String at(int index) {
		return values[bytes.get(index) & 0xFF];
	}

	@Override
	String getNoOffset(int index) {

		if (isNullNoOffset(index))
			return null;
		else
			return at(index);
	}

	@Override
	boolean isNullNoOffset(int index) {
		return bytes.isNull(index);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof String;
	}

	@Override
	public ListIterator<String> listIterator(final int idx) {

		checkPositionIndex(idx, size);

		return new ImmutableListIterator<String>() {

			int index = idx + offset;

			@Override
			public boolean hasNext() {
				return index <= lastIndex();
			}

			@Override
			public String next() {
				if (!hasNext())
					throw new NoSuchElementException("called next when hasNext is false");

				if (!isNullNoOffset(index))
					return at(index++);
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
			public String previous() {
				if (!hasPrevious())
					throw new NoSuchElementException("called previous when hasPrevious is false");

				if (!isNullNoOffset(--index))
					return at(index);
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
	NormalStringColumnImpl subColumn0(int fromIndex, int toIndex) {
		return new NormalStringColumnImpl(bytes, indices, fromIndex + offset, toIndex - fromIndex);
	}

	private ByteColumn sliceBytes() {
		return bytes.subColumn(offset, offset + size);
	}

	@Override
	public NormalStringColumn copy() {
		return new NormalStringColumnImpl(sliceBytes().copy(), new HashMap<>(indices), 0, size);
	}

	@Override
	public NormalStringColumn toHeap() {
		return this;
	}

	@Override
	Column<String> applyFilter0(BufferBitSet keep, int cardinality) {

		@SuppressWarnings("rawtypes")
		AbstractColumn slice = (AbstractColumn) sliceBytes();
		ByteColumn bytes = (ByteColumn) slice.applyFilter(keep, cardinality);

		return new NormalStringColumnImpl(bytes, indices, 0, cardinality);
	}

	@Override
	Column<String> select0(IntColumn indices) {

		@SuppressWarnings("rawtypes")
		AbstractColumn slice = (AbstractColumn) sliceBytes();
		ByteColumn bytes = (ByteColumn) slice.select(indices);

		return new NormalStringColumnImpl(bytes, this.indices, 0, indices.size());
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		} else if (o instanceof NormalStringColumnImpl) {
			return equals0((NormalStringColumnImpl) o);
		} else if (o instanceof List) {
			return super.equals(o);
		} else {
			return false;
		}
	}

	@Override
	boolean equals0(NormalStringColumnImpl rhs) {

		if (size != rhs.size)
			return false;

		for (int i = 0; i < size; i++)
			if (!Objects.equals(getNoOffset(i + offset), rhs.getNoOffset(i + rhs.offset)))
				return false;

		return true;
	}

	@SuppressWarnings("rawtypes")
	@Override
	void writeTo(WritableByteChannel channel) throws IOException {

		((AbstractColumn) sliceBytes()).writeTo(channel);

		writeInt(channel, BIG_ENDIAN, values.length);
		for (int i = 0; i < values.length; i++) {
			writeInt(channel, BIG_ENDIAN, values[i].length());
			writeFully(channel, ByteBuffer.wrap(values[i].getBytes(StandardCharsets.UTF_8)));
		}
	}

	@Override
	NormalStringColumn append0(Column<String> tail) {

		return new NormalStringColumnBuilder().addAll(this).addAll(tail).build();
	}

	/*------------------------------------------------------------
	 *             Unsupported Sorted Set operations
	 *------------------------------------------------------------*/

	@Override
	public NavigableSet<String> asSet() {
		throw new UnsupportedOperationException("asSet");
	}

	@Override
	public String lower(String value) {
		throw new UnsupportedOperationException("lower");
	}

	@Override
	public String higher(String value) {
		throw new UnsupportedOperationException("higher");
	}

	@Override
	public String floor(String value) {
		throw new UnsupportedOperationException("floor");
	}

	@Override
	public String ceiling(String value) {
		throw new UnsupportedOperationException("ceiling");
	}

	@Override
	public NormalStringColumn subColumnByValue(String fromElement, boolean fromInclusive, String toElement,
			boolean toInclusive) {
		throw new UnsupportedOperationException("subColumnByValue");
	}

	@Override
	public NormalStringColumn subColumnByValue(String fromElement, String toElement) {
		throw new UnsupportedOperationException("subColumnByValue");
	}

	@Override
	public NormalStringColumn head(String toElement, boolean inclusive) {
		throw new UnsupportedOperationException("head");
	}

	@Override
	public NormalStringColumn head(String toElement) {
		throw new UnsupportedOperationException("lower");
	}

	@Override
	public NormalStringColumn tail(String fromElement, boolean inclusive) {
		throw new UnsupportedOperationException("tail");
	}

	@Override
	public NormalStringColumn tail(String fromElement) {
		throw new UnsupportedOperationException("tail");
	}

	@Override
	public NormalStringColumn toSorted() {
		throw new UnsupportedOperationException("toSorted");
	}

	@Override
	public NormalStringColumn toDistinct() {
		throw new UnsupportedOperationException("toDistinct");
	}

	@Override
	int intersectBothSorted(NormalStringColumnImpl rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectBothSorted");
	}

	@Override
	IntColumn intersectLeftSorted(NormalStringColumn rhs, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectLeftSorted");
	}

	/*------------------------------------------------------------
	 *                Column conversion methods
	 *------------------------------------------------------------*/

	@Override
	public BooleanColumn toBooleanColumn(Predicate<String> predicate) {

		BooleanColumnBuilder builder = new BooleanColumnBuilder();
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				boolean value = predicate.test(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public DateColumn toDateColumn(Function<String, LocalDate> function) {

		DateColumnBuilder builder = new DateColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				LocalDate value = function.apply(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public DateTimeColumn toDateTimeColumn(Function<String, LocalDateTime> function) {

		DateTimeColumnBuilder builder = new DateTimeColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				LocalDateTime value = function.apply(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public DoubleColumn toDoubleColumn(ToDoubleFunction<String> function) {

		DoubleColumnBuilder builder = new DoubleColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				double value = function.applyAsDouble(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public FloatColumn toFloatColumn(ToFloatFunction<String> function) {

		FloatColumnBuilder builder = new FloatColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				float value = function.applyAsFloat(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public IntColumn toIntColumn(ToIntFunction<String> function) {

		IntColumnBuilder builder = new IntColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				int value = function.applyAsInt(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public LongColumn toLongColumn(ToLongFunction<String> function) {

		LongColumnBuilder builder = new LongColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				long value = function.applyAsLong(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public ShortColumn toShortColumn(ToShortFunction<String> function) {

		ShortColumnBuilder builder = new ShortColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				short value = function.applyAsShort(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public ByteColumn toByteColumn(ToByteFunction<String> function) {

		ByteColumnBuilder builder = new ByteColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				byte value = function.applyAsByte(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public DecimalColumn toDecimalColumn(Function<String, BigDecimal> function) {

		DecimalColumnBuilder builder = new DecimalColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				BigDecimal value = function.apply(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public StringColumn toStringColumn(Function<String, String> function) {

		StringColumnBuilder builder = new StringColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				String value = function.apply(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public UuidColumn toUuidColumn(Function<String, UUID> function) {

		UuidColumnBuilder builder = new UuidColumnBuilder(0);
		builder.ensureCapacity(size);

		for (int i = offset; i <= lastIndex(); i++) {
			if (isNullNoOffset(i))
				builder.addNull();
			else {
				UUID value = function.apply(at(i));
				builder.add(value);
			}
		}

		return builder.build();
	}

	@Override
	public NormalStringColumn clean(Predicate<String> predicate) {
		throw new UnsupportedOperationException("clean");
	}

	@Override
	public NormalStringColumn filter(Predicate<String> predicate, boolean keepNulls) {

		boolean[] keep = new boolean[values.length];
		boolean keepAll = true;
		for (int i = 0; i < values.length; i++) {
			keep[i] = predicate.test(values[i]);
			keepAll = keepAll && keep[i];
		}
		if (keepAll)
			return this;

		ByteColumn bytes = this.bytes.subColumn(offset, offset + size).filter(b -> keep[b & 0xFF], keepNulls);

		if (bytes.size() == this.bytes.size())
			return this;
		else
			return new NormalStringColumnImpl(bytes, indices, 0, bytes.size());
	}
}
