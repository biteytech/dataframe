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

import java.time.LocalDate;
import java.util.NavigableMap;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Predicate;

/**
 * A time series specialization of {@link DataFrame}. A {@code DateSeries} is a
 * {@code DataFrame} with exactly two columns:
 * <ol>
 * <li>The first column must be a unique index of {@link ColumnType#DATE}.
 * <li>The second column must be a non-null column of {@link ColumnType#DOUBLE}.
 * </ol>
 * A {@link Row} in a time series can be thought of as a {@code (date,value)}
 * point.
 * 
 * @author biteytech@protonmail.com
 */
public interface DateSeries extends DataFrame {

	/**
	 * Returns a new time series where the values have been transformed by the
	 * provided operation.
	 * <p>
	 * The resulting time series will have the same column names.
	 * 
	 * @param op - the operation used to transform the values
	 * 
	 * @return a new time series where the values have been transformed by the
	 *         provided operation.
	 */
	DateSeries transform(DoubleUnaryOperator op);

	/**
	 * Joins this time series with the specifies series, and combines their values
	 * point-wise using the specified operation.
	 * <p>
	 * The resulting time series will have the same column names as this one.
	 * 
	 * @param series - the series to be joined with this one
	 * @param op     - the operation used to combine the values
	 * 
	 * @return the new joint time series with value combined using the specified
	 *         operation
	 */
	DateSeries transform(DateSeries series, DoubleBinaryOperator op);

	/**
	 * Returns an immutable {@link NavigableMap} view of this time series.
	 * 
	 * @return an immutable {@code NavigableMap} view of this time series.
	 */
	NavigableMap<LocalDate, Double> asSeriesMap();

	/**
	 * Returns the date for the point at the specified index.
	 * 
	 * @param index - the point/row index
	 * 
	 * @return the date for the point at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	default LocalDate getDate(int index) {
		return getDate(index, 0);
	}

	/**
	 * Returns the yyyymmdd-format date for the point at the specified index.
	 * 
	 * @param index - the point/row index
	 * 
	 * @return the yyyymmdd-format date for the point at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	default int yyyymmdd(int index) {
		return yyyymmdd(index, 0);
	}

	/**
	 * Returns the value for the point at the specified index.
	 * 
	 * @param index - the point/row index
	 * 
	 * @return the value for the point at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	default double getValue(int index) {
		return getDouble(index, 1);
	}

	/**
	 * Returns this time series
	 * 
	 * @param columnIndex - ignored
	 * 
	 * @return this time series
	 */
	@Override
	default DateSeries toDateSeries(int columnIndex) {
		return this;
	}

	/**
	 * Returns this time series
	 * 
	 * @param columnName - ignored
	 * 
	 * @return this time series
	 */
	@Override
	default DateSeries toDateSeries(String columnName) {
		return this;
	}

	/**
	 * Returns a {@link DateSeries} containing {@code sampleSize} points selected at
	 * random.
	 * 
	 * @param sampleSize - the number of points to sample from this time series
	 * 
	 * @return a dataframe containing {@code sampleSize} points selected at random.
	 * 
	 * @throws IndexOutOfBoundsException if {@code sampleSize} is negative or is
	 *                                   greater than {@code #size()}
	 */
	@Override
	DateSeries sampleN(int sampleSize);

	/**
	 * Returns a {@link DateSeries} containing {@code size()*proportion} points
	 * selected at random.
	 * 
	 * @param proportion - a number between 0.0 and 1.0 inclusive
	 * 
	 * @return a dataframe containing {@code size()*proportion} points selected at
	 *         random.
	 */
	@Override
	DateSeries sampleX(double proportion);

	/**
	 * Returns a {@link DateSeries} containing the points which pass the specified
	 * {@link Predicate}.
	 * 
	 * @param criteria - the {@code Predicate} used to filter the points in this
	 *                 time series
	 * 
	 * @return a {@code DateSeries} containing the points which pass the specified
	 *         {@code Predicate}.
	 */
	@Override
	DateSeries filter(Predicate<Row> criteria);

	/**
	 * Appends the specified {@link DateSeries} to this one by calling
	 * {@link Column#append(Column)} on both the key and value columns, and keeping
	 * the meta-data from this dataframe.
	 * 
	 * @param df - the time series to be appended to this one
	 * 
	 * @return a new {@code DateSeries} resulting from appended the specified time
	 *         series to this one.
	 */
	DateSeries append(DateSeries df);

	/**
	 * Returns a {@link DateSeries} containing the first {@code count} points from
	 * this time series, or this time series if it contains fewer than {@code count}
	 * points.
	 * 
	 * @param count - number of points to return from the "top" or "head" of this
	 *              time series.
	 * 
	 * @return a {@code DateSeries} containing the first {@code count} points from
	 *         this time series.
	 */
	@Override
	DateSeries head(int count);

	/**
	 * Returns an empty {@link DateSeries}. The resulting time series will have the
	 * same meta-data (column names) as this one, but the key and value columns will
	 * be empty.
	 * 
	 * @return an empty {@code DateSeries} with the same meta-data as this one.
	 */
	@Override
	DateSeries empty();

	/**
	 * Returns a {@link DateSeries} containing the first 10 points from this time
	 * series, or this time series if it contains fewer than 10 points.
	 * 
	 * @return a {@code DateSeries} containing the first 10 points from this time
	 *         series.
	 */
	@Override
	DateSeries head();

	/**
	 * Returns a {@link DateSeries} containing the last {@code count} points from
	 * this dataframe, or this time series if it contains fewer than {@code count}
	 * points.
	 * 
	 * @param count - number of points to return from the "end" or "tail" of this
	 *              time series.
	 * 
	 * @return a {@code DateSeries} containing the last {@code count} points from
	 *         this time series.
	 */
	@Override
	DateSeries tail(int count);

	/**
	 * Returns a {@link DateSeries} containing the last 10 points from this time
	 * series, or this time series if it contains fewer than 10 points.
	 * 
	 * @return a {@code DateSeries} containing the last 10 points from this time
	 *         series.
	 */
	@Override
	DateSeries tail();

	/**
	 * Returns a {@link DateSeries} containing the points between the specified
	 * <tt>fromIndex</tt>, inclusive, and <tt>toIndex</tt>, exclusive (if
	 * <tt>fromIndex</tt> and <tt>toIndex</tt> are equal, the returned time series
	 * is empty).
	 * 
	 * @param fromIndex - index of the lowest point (inclusive)
	 * @param toIndex   - index of the highest point (exclusive)
	 * 
	 * @return a {@code DateSeries} containing the points between the specified
	 *         <tt>fromIndex</tt>, inclusive, and <tt>toIndex</tt>, exclusive
	 * 
	 * @throws IndexOutOfBoundsException if either index is negative or is greater
	 *                                   than {@link #size()}, or if {@code toIndex}
	 *                                   is less than {@code fromIndex}
	 */
	DateSeries subSeries(int fromIndex, int toIndex);

	/**
	 * Returns a {@link DateSeries} containing the points with dates strictly less
	 * than {@code toKey}.
	 *
	 * @param toKey high endpoint (exclusive) of the dates in the returned time
	 *              series
	 * 
	 * @return the points from this time series whose dates are strictly less than
	 *         {@code toKey}.
	 * 
	 * @throws NullPointerException if {@code toKey} is null
	 */
	DateSeries headTo(LocalDate toKey);

	/**
	 * Returns a {@link DateSeries} containing the points with dates greater than or
	 * equal to {@code fromKey}.
	 *
	 * @param fromKey low endpoint (inclusive) of the dates in the returned time
	 *                series
	 * 
	 * @return the points from this dataframe whose dates are greater than or equal
	 *         to {@code fromKey}.
	 * 
	 * @throws NullPointerException if {@code fromKey} is null
	 */
	DateSeries tailFrom(LocalDate fromKey);

	/**
	 * Returns a {@link DateSeries} containing the points with dates are greater
	 * than or equal to {@code fromKey} and strictly less than {@code toKey}.
	 *
	 * @param fromKey low endpoint (inclusive) of the dates in the returned time
	 *                series
	 * @param toKey   high endpoint (exclusive) of the dates in the returned time
	 *                series
	 * 
	 * @return the points from this time series whose dates are greater than or
	 *         equal to {@code fromKey} and strictly less than {@code toKey}.
	 * 
	 * @throws NullPointerException if either key is null
	 */
	DateSeries subSeries(LocalDate fromKey, LocalDate toKey);

	/**
	 * Returns a {@link DateSeries} containing the points with dates between
	 * {@code fromKey} and {@code toKey}. If {@code fromKey} and {@code toKey} are
	 * equal, the resulting time series is empty unless {@code
	 * fromInclusive} and {@code toInclusive} are both true.
	 *
	 * @param fromKey       low endpoint (inclusive) of the dates in the returned
	 *                      time series
	 * @param fromInclusive true if the low endpoint is to be included in the result
	 * @param toKey         high endpoint (exclusive) of the dates in the returned
	 *                      time series
	 * @param toInclusive   true if the high endpoint is to be included in the
	 *                      result
	 * 
	 * @return the points from this dataframe whose dates are between
	 *         {@code fromKey} and {@code toKey}.
	 * 
	 * @throws NullPointerException if either key is null
	 */
	DateSeries subSeries(LocalDate fromKey, boolean fromInclusive, LocalDate toKey, boolean toInclusive);
}
