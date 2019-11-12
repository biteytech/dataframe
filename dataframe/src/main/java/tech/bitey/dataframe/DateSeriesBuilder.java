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

import static java.util.Spliterator.NONNULL;
import static tech.bitey.dataframe.DateSeriesFactory.DEFAULT_DATE_COLUMN_NAME;
import static tech.bitey.dataframe.DateSeriesFactory.DEFAULT_VALUE_COLUMN_NAME;

import java.time.LocalDate;

/**
 * A builder for creating {@link DateSeries} instances. Example:
 *
 * <pre>
 * DateSeries ds = DateSeriesBuilder.builder().add(LocalDate.now(), 1).add(LocalDate.now().plusDays(1), 2).build();
 * </pre>
 * 
 * The resulting {@code DateSeries} will be sorted by date regardless of the
 * order the points were added in.
 * <p>
 * Builder instances can be reused; it is safe to call
 * {@link DateSeriesBuilder#build build} multiple times to build multiple
 * time series. Each new series contains all the points of the ones created
 * before it.
 *
 * @author biteytech@protonmail.com
 */
public class DateSeriesBuilder {

	private final DateColumnBuilder datesBuilder = new DateColumnBuilder(NONNULL);
	private final DoubleColumnBuilder valuesBuilder = new DoubleColumnBuilder(NONNULL);

	private final String dateColumnName;
	private final String valueColumnName;

	/**
	 * Create a bulider with the default column names ("DATES" and "VALUES").
	 */
	public DateSeriesBuilder() {
		this(DEFAULT_DATE_COLUMN_NAME, DEFAULT_VALUE_COLUMN_NAME);
	}

	/**
	 * Create a bulider with the specified column names.
	 * 
	 * @param dateColumnName  - the date column name
	 * @param valueColumnName - the value column name
	 */
	public DateSeriesBuilder(String dateColumnName, String valueColumnName) {
		this.dateColumnName = dateColumnName;
		this.valueColumnName = valueColumnName;
	}

	/**
	 * Add a point to this time series.
	 * 
	 * @param year  - the year
	 * @param month - the month (1-based)
	 * @param day   - the day (1-based)
	 * @param value - the value
	 * 
	 * @return this builder
	 */
	public DateSeriesBuilder add(int year, int month, int day, double value) {
		datesBuilder.add(year, month, day);
		valuesBuilder.add(value);
		return this;
	}

	/**
	 * Add a point to this time series.
	 * 
	 * @param yyyymmdd - the date in {@code yyyymmdd} format
	 * @param value    - the value
	 * 
	 * @return this builder
	 */
	public DateSeriesBuilder add(int yyyymmdd, double value) {
		return add(yyyymmdd / 10000, yyyymmdd % 10000 / 100, yyyymmdd % 100, value);
	}

	/**
	 * Add a point to this time series.
	 * 
	 * @param date  - the date
	 * @param value - the value
	 * 
	 * @return this builder
	 * 
	 * @throws NullPointerException if {@code date} is null
	 */
	public DateSeriesBuilder add(LocalDate date, double value) {
		datesBuilder.add(date);
		valuesBuilder.add(value);
		return this;
	}

	/**
	 * Increases the capacity of this {@code DateSeriesBuilder} instance, if
	 * necessary, to ensure that it can hold at least the number of points specified
	 * by the minimum capacity argument.
	 *
	 * @param minCapacity the desired minimum capacity
	 * 
	 * @return this builder
	 */
	DateSeriesBuilder ensureCapacity(int minCapacity) {
		datesBuilder.ensureCapacity(minCapacity);
		valuesBuilder.ensureCapacity(minCapacity);
		return this;
	}

	/**
	 * Sorts the added points by date and returns a new {@link DateSeries}.
	 * 
	 * @return a new {@code DateSeries}
	 * 
	 * @throws IllegalStateException if the dates are not all distinct
	 */
	public DateSeries build() {

		NonNullDateColumn dates = (NonNullDateColumn) datesBuilder.build();
		NonNullDateColumn distinctDates = dates.toDistinct();

		NonNullDoubleColumn values = (NonNullDoubleColumn) valuesBuilder.build();

		if (dates != distinctDates) {

			IntColumn indices = dates.sortIndices(distinctDates);
			values = values.select0(indices);
		}

		return new DateSeriesImpl(distinctDates, dateColumnName, values, valueColumnName);
	}
}
