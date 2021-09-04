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
import static java.util.Spliterator.NONNULL;
import static tech.bitey.bufferstuff.BufferUtils.readFully;
import static tech.bitey.dataframe.AbstractColumn.readInt;
import static tech.bitey.dataframe.ColumnTypeCode.B;
import static tech.bitey.dataframe.ColumnTypeCode.BD;
import static tech.bitey.dataframe.ColumnTypeCode.D;
import static tech.bitey.dataframe.ColumnTypeCode.DA;
import static tech.bitey.dataframe.ColumnTypeCode.DT;
import static tech.bitey.dataframe.ColumnTypeCode.F;
import static tech.bitey.dataframe.ColumnTypeCode.I;
import static tech.bitey.dataframe.ColumnTypeCode.L;
import static tech.bitey.dataframe.ColumnTypeCode.NS;
import static tech.bitey.dataframe.ColumnTypeCode.S;
import static tech.bitey.dataframe.ColumnTypeCode.T;
import static tech.bitey.dataframe.ColumnTypeCode.TI;
import static tech.bitey.dataframe.ColumnTypeCode.UU;
import static tech.bitey.dataframe.ColumnTypeCode.Y;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
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
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public class ColumnType<E extends Comparable<? super E>> {

	/** The type for {@link BooleanColumn} */
	public static final ColumnType<Boolean> BOOLEAN = new ColumnType<>(B);

	/** The type for {@link DateColumn} */
	public static final ColumnType<LocalDate> DATE = new ColumnType<>(DA);

	/** The type for {@link DateTimeColumn} */
	public static final ColumnType<LocalDateTime> DATETIME = new ColumnType<>(DT);

	/** The type for {@link DateTimeColumn} */
	public static final ColumnType<LocalTime> TIME = new ColumnType<>(TI);

	/** The type for {@link DoubleColumn} */
	public static final ColumnType<Double> DOUBLE = new ColumnType<>(D);

	/** The type for {@link FloatColumn} */
	public static final ColumnType<Float> FLOAT = new ColumnType<>(F);

	/** The type for {@link IntColumn} */
	public static final ColumnType<Integer> INT = new ColumnType<>(I);

	/** The type for {@link LongColumn} */
	public static final ColumnType<Long> LONG = new ColumnType<>(L);

	/** The type for {@link ShortColumn} */
	public static final ColumnType<Short> SHORT = new ColumnType<>(T);

	/** The type for {@link StringColumn} */
	public static final ColumnType<String> STRING = new ColumnType<>(S);

	/** The type for {@link StringColumn} */
	public static final ColumnType<Byte> BYTE = new ColumnType<>(Y);

	/** The type for {@link DecimalColumn} */
	public static final ColumnType<BigDecimal> DECIMAL = new ColumnType<>(BD);

	/** The type for {@link UuidColumn} */
	public static final ColumnType<UUID> UUID = new ColumnType<>(UU);

	/** The type for {@link NormalStringColumn} */
	public static final ColumnType<String> NSTRING = new ColumnType<>(NS);

	private final ColumnTypeCode code;

	private ColumnType(ColumnTypeCode code) {
		this.code = code;
	}

	public ColumnTypeCode getCode() {
		return code;
	}

	/**
	 * Returns a {@link ColumnBuilder builder} for this column type.
	 * 
	 * @param <T> the corresponding element type
	 * 
	 * @return a {@link ColumnBuilder builder} for this column type.
	 */
	public <T extends Comparable<? super T>> ColumnBuilder<T> builder() {
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
	public <T extends Comparable<? super T>> ColumnBuilder<T> builder(int characteristic) {
		switch (getCode()) {
		case B:
			return (ColumnBuilder) BooleanColumn.builder();
		case DA:
			return (ColumnBuilder) DateColumn.builder(characteristic);
		case DT:
			return (ColumnBuilder) DateTimeColumn.builder(characteristic);
		case TI:
			return (ColumnBuilder) TimeColumn.builder(characteristic);
		case D:
			return (ColumnBuilder) DoubleColumn.builder(characteristic);
		case F:
			return (ColumnBuilder) FloatColumn.builder(characteristic);
		case I:
			return (ColumnBuilder) IntColumn.builder(characteristic);
		case L:
			return (ColumnBuilder) LongColumn.builder(characteristic);
		case T:
			return (ColumnBuilder) ShortColumn.builder(characteristic);
		case Y:
			return (ColumnBuilder) ByteColumn.builder(characteristic);
		case S:
			return (ColumnBuilder) StringColumn.builder(characteristic);
		case BD:
			return (ColumnBuilder) DecimalColumn.builder(characteristic);
		case UU:
			return (ColumnBuilder) UuidColumn.builder(characteristic);
		case NS:
			return (ColumnBuilder) NormalStringColumn.builder();
		}

		throw new IllegalStateException();
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

	Column<?> readFrom(ReadableByteChannel channel, int characteristics) throws IOException {
		BufferBitSet nonNulls = null;
		int size = 0;
		if (getCode() != NS && !((characteristics & NONNULL) != 0)) {
			size = readInt(channel, BIG_ENDIAN);
			nonNulls = BufferBitSet.readFrom(channel);
		}

		switch (getCode()) {
		case B: {
			NonNullBooleanColumn column = NonNullBooleanColumn.EMPTY.readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableBooleanColumn(column, nonNulls, null, 0, size);
		}
		case DA: {
			NonNullDateColumn column = NonNullDateColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableDateColumn(column, nonNulls, null, 0, size);
		}
		case DT: {
			NonNullDateTimeColumn column = NonNullDateTimeColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableDateTimeColumn(column, nonNulls, null, 0, size);
		}
		case TI: {
			NonNullTimeColumn column = NonNullTimeColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableTimeColumn(column, nonNulls, null, 0, size);
		}
		case D: {
			NonNullDoubleColumn column = NonNullDoubleColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableDoubleColumn(column, nonNulls, null, 0, size);
		}
		case F: {
			NonNullFloatColumn column = NonNullFloatColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableFloatColumn(column, nonNulls, null, 0, size);
		}
		case I: {
			NonNullIntColumn column = NonNullIntColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableIntColumn(column, nonNulls, null, 0, size);
		}
		case L: {
			NonNullLongColumn column = NonNullLongColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableLongColumn(column, nonNulls, null, 0, size);
		}
		case T: {
			NonNullShortColumn column = NonNullShortColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableShortColumn(column, nonNulls, null, 0, size);
		}
		case Y: {
			NonNullByteColumn column = NonNullByteColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableByteColumn(column, nonNulls, null, 0, size);
		}
		case S: {
			NonNullStringColumn column = NonNullStringColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableStringColumn(column, nonNulls, null, 0, size);
		}
		case BD: {
			NonNullDecimalColumn column = NonNullDecimalColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableDecimalColumn(column, nonNulls, null, 0, size);
		}
		case UU: {
			NonNullUuidColumn column = NonNullUuidColumn.empty(characteristics).readFrom(channel);
			if (nonNulls == null)
				return column;
			else
				return new NullableUuidColumn(column, nonNulls, null, 0, size);
		}
		case NS: {
			ByteColumn bytes = (ByteColumn) Y.getType().readFrom(channel, characteristics);

			Map<String, Integer> indices = new HashMap<>();
			int count = readInt(channel, BIG_ENDIAN);
			for (int i = 0; i < count; i++) {

				ByteBuffer buf = ByteBuffer.allocate(readInt(channel, BIG_ENDIAN));
				readFully(channel, buf);
				String value = new String(buf.array(), StandardCharsets.UTF_8);

				indices.put(value, i);
			}

			return new NormalStringColumnImpl(bytes, indices, 0, bytes.size());
		}
		}

		throw new IllegalStateException();
	}

	@SuppressWarnings("rawtypes")
	NullableColumnConstructor nullableConstructor() {
		switch (getCode()) {
		case B:
			return NullableBooleanColumn::new;
		case BD:
			return NullableDecimalColumn::new;
		case D:
			return NullableDoubleColumn::new;
		case DA:
			return NullableDateColumn::new;
		case DT:
			return NullableDateTimeColumn::new;
		case TI:
			return NullableTimeColumn::new;
		case F:
			return NullableFloatColumn::new;
		case I:
			return NullableIntColumn::new;
		case L:
			return NullableLongColumn::new;
		case S:
			return NullableStringColumn::new;
		case T:
			return NullableShortColumn::new;
		case UU:
			return NullableUuidColumn::new;
		case Y:
			return NullableByteColumn::new;
		case NS:
			// should never be called
		}

		throw new IllegalStateException();
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
	public Comparable<?> parse(String string) {
		switch (getCode()) {
		case B:
			return Boolean.valueOf(parseBoolean(string));
		case DA:
			return parseDate(string);
		case DT:
			return LocalDateTime.parse(string);
		case TI:
			return LocalTime.parse(string);
		case D:
			return Double.valueOf(string);
		case F:
			return Float.valueOf(string);
		case I:
			return Integer.valueOf(string);
		case L:
			return Long.valueOf(string);
		case T:
			return Short.valueOf(string);
		case Y:
			return Byte.valueOf(string);
		case S:
		case NS:
			return string;
		case BD:
			return new BigDecimal(string);
		case UU:
			return java.util.UUID.fromString(string);
		}
		throw new IllegalStateException();
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

	@Override
	public String toString() {
		return code.toString();
	}
}
