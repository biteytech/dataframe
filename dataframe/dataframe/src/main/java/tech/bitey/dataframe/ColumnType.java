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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Spliterator.NONNULL;
import static tech.bitey.bufferstuff.BufferUtils.readFully;
import static tech.bitey.dataframe.AbstractColumn.readInt;
import static tech.bitey.dataframe.ColumnTypeCode.B;
import static tech.bitey.dataframe.ColumnTypeCode.BD;
import static tech.bitey.dataframe.ColumnTypeCode.BL;
import static tech.bitey.dataframe.ColumnTypeCode.D;
import static tech.bitey.dataframe.ColumnTypeCode.DA;
import static tech.bitey.dataframe.ColumnTypeCode.DT;
import static tech.bitey.dataframe.ColumnTypeCode.F;
import static tech.bitey.dataframe.ColumnTypeCode.I;
import static tech.bitey.dataframe.ColumnTypeCode.IN;
import static tech.bitey.dataframe.ColumnTypeCode.L;
import static tech.bitey.dataframe.ColumnTypeCode.NS;
import static tech.bitey.dataframe.ColumnTypeCode.S;
import static tech.bitey.dataframe.ColumnTypeCode.T;
import static tech.bitey.dataframe.ColumnTypeCode.TI;
import static tech.bitey.dataframe.ColumnTypeCode.UU;
import static tech.bitey.dataframe.ColumnTypeCode.Y;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * Represents the possible element types supported by the concrete
 * {@link Column} implementation. One of:
 * <ul>
 * <li>{@link #BOOLEAN}
 * <li>{@link #DATE}
 * <li>{@link #DATETIME}
 * <li>{@link #TIME}
 * <li>{@link #INSTANT}
 * <li>{@link #DOUBLE}
 * <li>{@link #FLOAT}
 * <li>{@link #INT}
 * <li>{@link #LONG}
 * <li>{@link #SHORT}
 * <li>{@link #STRING}
 * <li>{@link #BYTE}
 * <li>{@link #DECIMAL}
 * <li>{@link #UUID}
 * <li>{@link #NSTRING}
 * <li>{@link #BLOB}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public abstract class ColumnType<E> {

	/** The type for {@link BooleanColumn} */
	public static final ColumnType<Boolean> BOOLEAN = new ColumnType<>(B, Boolean.class) {

		@Override
		public int compare(Boolean lhs, Boolean rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link DateColumn} */
	public static final ColumnType<LocalDate> DATE = new ColumnType<>(DA, LocalDate.class) {

		@Override
		public int compare(LocalDate lhs, LocalDate rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link DateTimeColumn} */
	public static final ColumnType<LocalDateTime> DATETIME = new ColumnType<>(DT, LocalDateTime.class) {

		@Override
		public int compare(LocalDateTime lhs, LocalDateTime rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link DateTimeColumn} */
	public static final ColumnType<LocalTime> TIME = new ColumnType<>(TI, LocalTime.class) {

		@Override
		public int compare(LocalTime lhs, LocalTime rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link InstantColumn} */
	public static final ColumnType<Instant> INSTANT = new ColumnType<>(IN, Instant.class) {

		@Override
		public int compare(Instant lhs, Instant rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link DoubleColumn} */
	public static final ColumnType<Double> DOUBLE = new ColumnType<>(D, Double.class) {

		@Override
		public int compare(Double lhs, Double rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link FloatColumn} */
	public static final ColumnType<Float> FLOAT = new ColumnType<>(F, Float.class) {

		@Override
		public int compare(Float lhs, Float rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link IntColumn} */
	public static final ColumnType<Integer> INT = new ColumnType<>(I, Integer.class) {

		@Override
		public int compare(Integer lhs, Integer rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link LongColumn} */
	public static final ColumnType<Long> LONG = new ColumnType<>(L, Long.class) {

		@Override
		public int compare(Long lhs, Long rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link ShortColumn} */
	public static final ColumnType<Short> SHORT = new ColumnType<>(T, Short.class) {

		@Override
		public int compare(Short lhs, Short rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link StringColumn} */
	public static final ColumnType<String> STRING = new ColumnType<>(S, String.class) {

		@Override
		public int compare(String lhs, String rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link StringColumn} */
	public static final ColumnType<Byte> BYTE = new ColumnType<>(Y, Byte.class) {

		@Override
		public int compare(Byte lhs, Byte rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link DecimalColumn} */
	public static final ColumnType<BigDecimal> DECIMAL = new ColumnType<>(BD, BigDecimal.class) {

		@Override
		public int compare(BigDecimal lhs, BigDecimal rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link UuidColumn} */
	public static final ColumnType<UUID> UUID = new ColumnType<>(UU, UUID.class) {

		@Override
		public int compare(java.util.UUID lhs, java.util.UUID rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link NormalStringColumn} */
	public static final ColumnType<String> NSTRING = new ColumnType<>(NS, String.class) {

		@Override
		public int compare(String lhs, String rhs) {
			return lhs.compareTo(rhs);
		}
	};

	/** The type for {@link BlobColumn} */
	public static final ColumnType<InputStream> BLOB = new ColumnType<>(BL, InputStream.class) {

		@Override
		public int compare(InputStream lhs, InputStream rhs) {
			throw new UnsupportedOperationException();
		}
	};

	private final ColumnTypeCode code;
	private final Class<E> elementType;

	private ColumnType(ColumnTypeCode code, Class<E> elementType) {
		this.code = code;
		this.elementType = elementType;
	}

	public ColumnTypeCode getCode() {
		return code;
	}

	public Class<E> getElementType() {
		return elementType;
	}

	/**
	 * Returns a {@link ColumnBuilder builder} for this column type.
	 * 
	 * @param <T> the corresponding element type
	 * 
	 * @return a {@link ColumnBuilder builder} for this column type.
	 */
	public <T> ColumnBuilder<T> builder() {
		return builder(0);
	}

	/**
	 * Returns a {@link ColumnBuilder builder} for this column type with the
	 * specified characteristic.
	 * 
	 * @param characteristic - one of:
	 *                       <ul>
	 *                       <li>{@code 0} (zero) - no constraints on the elements
	 *                       to be added to the column
	 *                       <li>{@link java.util.Spliterator#NONNULL NONNULL}
	 *                       <li>{@link java.util.Spliterator#SORTED SORTED}
	 *                       <li>{@link java.util.Spliterator#DISTINCT DISTINCT}
	 *                       </ul>
	 * 
	 * @param <T>            the column's element type
	 * 
	 * @return a {@link ColumnBuilder builder} for this column type.
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> ColumnBuilder<T> builder(int characteristic) {
		return (ColumnBuilder) switch (getCode()) {
		case B -> BooleanColumn.builder();
		case DA -> DateColumn.builder(characteristic);
		case DT -> DateTimeColumn.builder(characteristic);
		case TI -> TimeColumn.builder(characteristic);
		case IN -> InstantColumn.builder(characteristic);
		case D -> DoubleColumn.builder(characteristic);
		case F -> FloatColumn.builder(characteristic);
		case I -> IntColumn.builder(characteristic);
		case L -> LongColumn.builder(characteristic);
		case T -> ShortColumn.builder(characteristic);
		case Y -> ByteColumn.builder(characteristic);
		case S -> StringColumn.builder(characteristic);
		case BD -> DecimalColumn.builder(characteristic);
		case UU -> UuidColumn.builder(characteristic);
		case NS -> NormalStringColumn.builder();
		case BL -> BlobColumn.builder();
		};
	}

	/**
	 * Returns a {@link Column} of the corresponding type that only contains nulls.
	 * 
	 * @param size the number of nulls in the column
	 * 
	 * @return a {@code Column} of the corresponding type that only contains nulls.
	 */
	public Column<?> nullColumn(int size) {
		return builder().addNulls(size).build();
	}

	Column<?> readFrom(ReadableByteChannel channel, int characteristics, int version) throws IOException {
		BufferBitSet nonNulls = null;
		int size = 0;
		if (getCode() != NS && !((characteristics & NONNULL) != 0)) {
			size = readInt(channel, BIG_ENDIAN);
			nonNulls = BufferBitSet.readFrom(channel);
		}

		return switch (getCode()) {
		case B -> {
			NonNullBooleanColumn column = NonNullBooleanColumn.EMPTY.readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableBooleanColumn(column, nonNulls, null, 0, size);
		}
		case DA -> {
			NonNullDateColumn column = NonNullDateColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableDateColumn(column, nonNulls, null, 0, size);
		}
		case DT -> {
			NonNullDateTimeColumn column = NonNullDateTimeColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableDateTimeColumn(column, nonNulls, null, 0, size);
		}
		case TI -> {
			NonNullTimeColumn column = NonNullTimeColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableTimeColumn(column, nonNulls, null, 0, size);
		}
		case IN -> {
			NonNullInstantColumn column = NonNullInstantColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableInstantColumn(column, nonNulls, null, 0, size);
		}
		case D -> {
			NonNullDoubleColumn column = NonNullDoubleColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableDoubleColumn(column, nonNulls, null, 0, size);
		}
		case F -> {
			NonNullFloatColumn column = NonNullFloatColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableFloatColumn(column, nonNulls, null, 0, size);
		}
		case I -> {
			NonNullIntColumn column = NonNullIntColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableIntColumn(column, nonNulls, null, 0, size);
		}
		case L -> {
			NonNullLongColumn column = NonNullLongColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableLongColumn(column, nonNulls, null, 0, size);
		}
		case T -> {
			NonNullShortColumn column = NonNullShortColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableShortColumn(column, nonNulls, null, 0, size);
		}
		case Y -> {
			NonNullByteColumn column = NonNullByteColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableByteColumn(column, nonNulls, null, 0, size);
		}
		case S -> {
			NonNullStringColumn column = NonNullStringColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableStringColumn(column, nonNulls, null, 0, size);
		}
		case BD -> {
			NonNullDecimalColumn column = NonNullDecimalColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableDecimalColumn(column, nonNulls, null, 0, size);
		}
		case BL -> {
			NonNullBlobColumn column = NonNullBlobColumn.EMPTY.readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableBlobColumn(column, nonNulls, null, 0, size);
		}
		case UU -> {
			NonNullUuidColumn column = NonNullUuidColumn.empty(characteristics).readFrom(channel, version);
			if (nonNulls == null)
				yield column;
			else
				yield new NullableUuidColumn(column, nonNulls, null, 0, size);
		}
		case NS -> {
			final ColumnTypeCode colTypeCode;
			if (version < 5)
				colTypeCode = Y;
			else {
				ByteBuffer buf = ByteBuffer.allocate(1);
				channel.read(buf);
				// Y (byte) or T (short)
				colTypeCode = ColumnTypeCode.valueOf(new String(new byte[] { buf.get(0) }));
			}

			final Column<? extends Number> indices;
			if (colTypeCode == Y)
				indices = (ByteColumn) BYTE.readFrom(channel, characteristics, version);
			else
				indices = (ShortColumn) SHORT.readFrom(channel, characteristics, version);

			final NonNullStringColumn values;
			if (version == 1) {
				StringColumnBuilder builder = StringColumn.builder(NONNULL);
				int count = readInt(channel, BIG_ENDIAN);
				for (int i = 0; i < count; i++) {

					ByteBuffer buf = ByteBuffer.allocate(readInt(channel, BIG_ENDIAN));
					readFully(channel, buf);
					String value = new String(buf.array(), StandardCharsets.UTF_8);

					builder.add(value);
				}
				values = (NonNullStringColumn) builder.build();
			} else {
				values = (NonNullStringColumn) STRING.readFrom(channel, NONNULL, version);
			}

			if (colTypeCode == Y)
				yield new NormalStringColumnByteImpl((ByteColumn) indices, values, 0, indices.size());
			else
				yield new NormalStringColumnShortImpl((ShortColumn) indices, values, 0, indices.size());
		}
		};
	}

	@SuppressWarnings("rawtypes")
	NullableColumnConstructor nullableConstructor() {
		return switch (getCode()) {
		case B -> NullableBooleanColumn::new;
		case BD -> NullableDecimalColumn::new;
		case D -> NullableDoubleColumn::new;
		case DA -> NullableDateColumn::new;
		case DT -> NullableDateTimeColumn::new;
		case TI -> NullableTimeColumn::new;
		case IN -> NullableInstantColumn::new;
		case F -> NullableFloatColumn::new;
		case I -> NullableIntColumn::new;
		case L -> NullableLongColumn::new;
		case S -> NullableStringColumn::new;
		case T -> NullableShortColumn::new;
		case UU -> NullableUuidColumn::new;
		case Y -> NullableByteColumn::new;
		case NS -> throw new IllegalStateException();
		case BL -> NullableBlobColumn::new;
		};
	}

	/**
	 * Parse a string to an element according to the following logic:
	 * 
	 * <table border=1>
	 * <tr>
	 * <th>Column Type</th>
	 * <th>Element Type</th>
	 * <th>Logic</th>
	 * </tr>
	 * <tr>
	 * <td>BOOLEAN</td>
	 * <td>{@link Boolean}</td>
	 * <td>{@code TRUE} if string equals "true" or "Y", ignoring case</td>
	 * </tr>
	 * <tr>
	 * <td>DATE</td>
	 * <td>{@link LocalDate}</td>
	 * <td>
	 * <ul>
	 * <li>if string has length 8, treat as {@code yyyymmdd} int value
	 * <li>otherwise, {@link LocalDate#parse(CharSequence)}
	 * </ul>
	 * </td>
	 * </tr>
	 * <tr>
	 * <td>DATETIME</td>
	 * <td>{@link LocalDateTime}</td>
	 * <td>{@link LocalDateTime#parse(CharSequence)}</td>
	 * </tr>
	 * <tr>
	 * <td>TIME</td>
	 * <td>{@link LocalTime}</td>
	 * <td>{@link LocalTime#parse(CharSequence)}</td>
	 * </tr>
	 * <tr>
	 * <td>DOUBLE</td>
	 * <td>{@link Double}</td>
	 * <td>{@link Double#valueOf(String)}</td>
	 * </tr>
	 * <tr>
	 * <td>FLOAT</td>
	 * <td>{@link Float}</td>
	 * <td>{@link Float#valueOf(String)}</td>
	 * </tr>
	 * <tr>
	 * <td>INT</td>
	 * <td>{@link Integer}</td>
	 * <td>{@link Integer#valueOf(String)}</td>
	 * </tr>
	 * <tr>
	 * <td>LONG</td>
	 * <td>{@link Long}</td>
	 * <td>{@link Long#valueOf(String)}</td>
	 * </tr>
	 * <tr>
	 * <td>STRING</td>
	 * <td>{@link String}</td>
	 * <td>as-is</td>
	 * </tr>
	 * <tr>
	 * <td>DECIMAL</td>
	 * <td>{@link BigDecimal}</td>
	 * <td>{@link BigDecimal#BigDecimal(String)}</td>
	 * </tr>
	 * <tr>
	 * <td>UUID</td>
	 * <td>{@link UUID}</td>
	 * <td>{@link UUID#fromString(String)}</td>
	 * </tr>
	 * </table>
	 * 
	 * @param string - the string to parse
	 * 
	 * @return the parsed element
	 */
	@SuppressWarnings("unchecked")
	public <T> T parse(String string) {
		return (T) switch (getCode()) {
		case B -> Boolean.valueOf(parseBoolean(string));
		case DA -> parseDate(string);
		case DT -> LocalDateTime.parse(string);
		case TI -> LocalTime.parse(string);
		case IN -> Instant.parse(string);
		case D -> Double.valueOf(string);
		case F -> Float.valueOf(string);
		case I -> Integer.valueOf(string);
		case L -> Long.valueOf(string);
		case T -> Short.valueOf(string);
		case Y -> Byte.valueOf(string);
		case S, NS -> string;
		case BD -> new BigDecimal(string);
		case UU -> java.util.UUID.fromString(string);
		case BL -> new ByteArrayInputStream(string.getBytes());
		};
	}

	public static boolean parseBoolean(String string) {
		return "true".equalsIgnoreCase(string) || "Y".equalsIgnoreCase(string);
	}

	public static LocalDate parseDate(String string) {
		if (string.length() == 8) {
			int yyyymmdd = Integer.parseInt(string);
			return LocalDate.of(yyyymmdd / 10000, yyyymmdd % 10000 / 100, yyyymmdd % 100);
		} else
			return LocalDate.parse(string);
	}

	public abstract int compare(E lhs, E rhs);

	@Override
	public String toString() {
		return code.toString();
	}
}
