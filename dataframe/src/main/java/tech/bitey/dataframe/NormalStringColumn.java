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

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link String}.
 * <p>
 * Elements are normalized by assigning a {@code byte} value to each distinct
 * element. So there can only be up to 256 distinct elements per column.
 * <p>
 * Does not support index operations (e.g. {@link #toSorted()},
 * {@link #toDistinct()}, etc.)
 * 
 * @author biteytech@protonmail.com
 */
public interface NormalStringColumn extends Column<String> {

	@Override
	NormalStringColumn subColumn(int fromIndex, int toIndex);

	@Override
	NormalStringColumn subColumnByValue(String fromElement, boolean fromInclusive, String toElement,
			boolean toInclusive);

	@Override
	NormalStringColumn subColumnByValue(String fromElement, String toElement);

	@Override
	NormalStringColumn head(String toElement, boolean inclusive);

	@Override
	NormalStringColumn head(String toElement);

	@Override
	NormalStringColumn tail(String fromElement, boolean inclusive);

	@Override
	NormalStringColumn tail(String fromElement);

	@Override
	NormalStringColumn toHeap();

	@Override
	NormalStringColumn toSorted();

	@Override
	NormalStringColumn toDistinct();

	@Override
	NormalStringColumn append(Column<String> tail);

	@Override
	NormalStringColumn copy();

	/**
	 * Returns a new {@link NormalStringColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link NormalStringColumnBuilder}
	 */
	public static NormalStringColumnBuilder builder() {
		return new NormalStringColumnBuilder();
	}

	/**
	 * Returns a new {@code NormalStringColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code NormalStringColumn} containing the specified elements.
	 */
	public static NormalStringColumn of(String... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Strings} into a new {@code NormalStringColumn}.
	 * 
	 * @return a new {@link NormalStringColumn}
	 */
	public static Collector<String, ?, NormalStringColumn> collector() {
		return Collector.of(NormalStringColumn::builder, NormalStringColumnBuilder::add,
				NormalStringColumnBuilder::append, NormalStringColumnBuilder::build);
	}

	/**
	 * Returns a new {@code NormalStringColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code NormalStringColumn} containing the specified elements.
	 */
	public static NormalStringColumn of(Collection<String> c) {
		return c.stream().collect(collector());
	}

	/**
	 * Convert this column into a {@link BooleanColumn} by applying the same logic
	 * as in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link BooleanColumn} parsed from this column.
	 */
	default BooleanColumn parseBoolean() {
		return toBooleanColumn(ColumnType::parseBoolean);
	}

	/**
	 * Convert this column into a {@link DateColumn} by applying the same logic as
	 * in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DateColumn} parsed from this column.
	 */
	default DateColumn parseDate() {
		return toDateColumn(ColumnType::parseDate);
	}

	/**
	 * Convert this column into a {@link DateColumn} by applying the specified
	 * {@link DateTimeFormatter} to each non-null element. Nulls are preserved
	 * as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DateColumn} parsed from this column.
	 */
	default DateColumn parseDate(DateTimeFormatter formatter) {
		return toDateColumn(text -> LocalDate.parse(text, formatter));
	}

	/**
	 * Convert this column into a {@link DateTimeColumn} by applying the same logic
	 * as in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DateTimeColumn} parsed from this column.
	 */
	default DateTimeColumn parseDateTime() {
		return toDateTimeColumn(LocalDateTime::parse);
	}

	/**
	 * Convert this column into a {@link DateTimeColumn} by applying the specified
	 * {@link DateTimeFormatter} to each non-null element. Nulls are preserved
	 * as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DateTimeColumn} parsed from this column.
	 */
	default DateTimeColumn parseDateTime(DateTimeFormatter formatter) {
		return toDateTimeColumn(text -> LocalDateTime.parse(text, formatter));
	}

	/**
	 * Convert this column into a {@link DoubleColumn} by applying the same logic as
	 * in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DoubleColumn} parsed from this column.
	 */
	default DoubleColumn parseDouble() {
		return toDoubleColumn(Double::parseDouble);
	}

	/**
	 * Convert this column into a {@link FloatColumn} by applying the same logic as
	 * in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link FloatColumn} parsed from this column.
	 */
	default FloatColumn parseFloat() {
		return toFloatColumn(Float::parseFloat);
	}

	/**
	 * Convert this column into a {@link IntColumn} by applying the same logic as in
	 * {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link IntColumn} parsed from this column.
	 */
	default IntColumn parseInt() {
		return toIntColumn(Integer::parseInt);
	}

	/**
	 * Convert this column into a {@link LongColumn} by applying the same logic as
	 * in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link LongColumn} parsed from this column.
	 */
	default LongColumn parseLong() {
		return toLongColumn(Long::parseLong);
	}

	/**
	 * Convert this column into a {@link ShortColumn} by applying the same logic as
	 * in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link ShortColumn} parsed from this column.
	 */
	default ShortColumn parseShort() {
		return toShortColumn(Short::parseShort);
	}

	/**
	 * Convert this column into a {@link ByteColumn} by applying the same logic as
	 * in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link ByteColumn} parsed from this column.
	 */
	default ByteColumn parseByte() {
		return toByteColumn(Byte::parseByte);
	}

	/**
	 * Convert this column into a {@link DecimalColumn} by applying the same logic
	 * as in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DecimalColumn} parsed from this column.
	 */
	default DecimalColumn parseDecimal() {
		return toDecimalColumn(BigDecimal::new);
	}

	/**
	 * Convert this column into a {@link UuidColumn} by applying the same logic as
	 * in {@link ColumnType#parse(String)} to each non-null element. Nulls are
	 * preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link UuidColumn} parsed from this column.
	 */
	default UuidColumn parseUuid() {
		return toUuidColumn(UUID::fromString);
	}

	@Override
	default StringColumn toStringColumn() {
		return toStringColumn(Function.identity());
	}

	public static void main(String[] args) throws Exception {
		NormalStringColumn column = builder().add(null, "a", "a", null, "c", "b", "c", null).build();
		column = column.subColumn(2, column.size() - 2);
		System.out.println(column);

		NormalStringColumn copy = column.copy();
		System.out.println(copy);
		System.out.println(column.equals(copy));

		DataFrame df = DataFrameFactory.create(new Column<?>[] { column, copy }, new String[] { "C1", "C2" });
		System.out.println(df.filterNulls());

		File file = new File("/home/lior/Desktop/test.dat");
		df.writeTo(file);
		DataFrame df2 = DataFrameFactory.readFrom(file);
		System.out.println(df.equals(df2));
	}
}
