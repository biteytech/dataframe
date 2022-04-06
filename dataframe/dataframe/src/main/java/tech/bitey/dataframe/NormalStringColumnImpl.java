package tech.bitey.dataframe;

import static tech.bitey.dataframe.Pr.checkPositionIndex;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ListIterator;
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

abstract class NormalStringColumnImpl<N extends Number, I extends Column<N>, C extends NormalStringColumnImpl<N, I, C>>
		extends AbstractColumn<String, NormalStringColumn, NormalStringColumnImpl<N, I, C>>
		implements NormalStringColumn {

	final I indices;
	final NonNullStringColumn values;

	NormalStringColumnImpl(I indices, NonNullStringColumn values, int offset, int size) {
		super(offset, size);

		this.indices = indices;
		this.values = values;
	}

	abstract String at(int index);

	abstract C constuct(I indices, NonNullStringColumn values, int offset, int size);

	@Override
	C subColumn0(int fromIndex, int toIndex) {
		return constuct(indices, values, fromIndex + offset, toIndex - fromIndex);
	}

	@SuppressWarnings("unchecked")
	@Override
	public NormalStringColumn copy() {
		return constuct((I) sliceIndices().copy(), values.copy(), 0, size);
	}

	@Override
	Column<String> applyFilter0(BufferBitSet keep, int cardinality) {

		@SuppressWarnings("rawtypes")
		AbstractColumn slice = (AbstractColumn) sliceIndices();
		@SuppressWarnings("unchecked")
		I indices = (I) slice.applyFilter(keep, cardinality);

		return constuct(indices, values, 0, cardinality);
	}

	@Override
	Column<String> select0(IntColumn indices) {

		@SuppressWarnings("rawtypes")
		AbstractColumn slice = (AbstractColumn) sliceIndices();
		@SuppressWarnings("unchecked")
		I column = (I) slice.select(indices);

		return constuct(column, this.values, 0, column.size());
	}

	@SuppressWarnings("unchecked")
	I sliceIndices() {
		return (I) indices.subColumn(offset, offset + size);
	}

	@Override
	public int characteristics() {
		return indices.characteristics();
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.NSTRING;
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
		return indices.isNull(index);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof String;
	}

	@Override
	boolean equals0(NormalStringColumnImpl<N, I, C> rhs) {

		if (size != rhs.size)
			return false;

		for (int i = 0; i < size; i++)
			if (!Objects.equals(getNoOffset(i + offset), rhs.getNoOffset(i + rhs.offset)))
				return false;

		return true;
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
	public NormalStringColumn toHeap() {
		return this;
	}

	@Override
	NormalStringColumn append0(Column<String> tail) {

		return new NormalStringColumnBuilder().addAll(this).addAll(tail).build();
	}

	@SuppressWarnings("rawtypes")
	@Override
	void writeTo(WritableByteChannel channel) throws IOException {

		channel.write(ByteBuffer.wrap(new byte[] { indices.getType().getCode().name().getBytes()[0] }));
		((AbstractColumn) sliceIndices()).writeTo(channel);

		values.writeTo(channel);
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
	IntColumn intersectLeftSorted(NormalStringColumn rhs, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectLeftSorted");
	}

	@Override
	int intersectBothSorted(NormalStringColumnImpl<N, I, C> rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectBothSorted");
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
}
