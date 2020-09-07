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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link String}.
 * <p>
 * Elements are stored as UTF-8 encoded bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface StringColumn extends Column<String> {

	@Override
	StringColumn subColumn(int fromIndex, int toIndex);

	@Override
	StringColumn subColumnByValue(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive);

	@Override
	StringColumn subColumnByValue(String fromElement, String toElement);

	@Override
	StringColumn head(String toElement, boolean inclusive);

	@Override
	StringColumn head(String toElement);

	@Override
	StringColumn tail(String fromElement, boolean inclusive);

	@Override
	StringColumn tail(String fromElement);

	@Override
	StringColumn toHeap();

	@Override
	StringColumn toSorted();

	@Override
	StringColumn toDistinct();

	@Override
	StringColumn append(Column<String> tail);

	@Override
	StringColumn copy();

	/**
	 * Returns an {@link StringColumnBuilder builder} with the specified
	 * characteristic.
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
	 * @return a new {@link StringColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static StringColumnBuilder builder(int characteristic) {
		return new StringColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link StringColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link StringColumnBuilder}
	 */
	public static StringColumnBuilder builder() {
		return new StringColumnBuilder(0);
	}

	/**
	 * Returns a new {@code StringColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code StringColumn} containing the specified elements.
	 */
	public static StringColumn of(String... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Strings} into a new {@code StringColumn} with the
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
	 * @return a new {@link StringColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<String, ?, StringColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), StringColumnBuilder::add, StringColumnBuilder::append,
				StringColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Strings} into a new {@code StringColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link StringColumn}
	 */
	public static Collector<String, ?, StringColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code StringColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code StringColumn} containing the specified elements.
	 */
	public static StringColumn of(Collection<String> c) {
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

	@Override
	default StringColumn toStringColumn() {
		return this;
	}
}
