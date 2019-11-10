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

import java.time.LocalDate;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

/**
 * A time series specialization of {@link DataFrame}. A {@code DateSeries} is a
 * {@code DataFrame} with exactly two columns:
 * <ol>
 * <li>The first column must be a unique index of {@link ColumnType#DATE}.
 * <li>The second column must be a non-null column of {@link ColumnType#DOUBLE}.
 * </ol>
 * A {@link Row} in a date-series can be thought of as a {@code (date,value)}
 * point.
 * 
 * @author biteytech@protonmail.com
 */
public interface DateSeries extends DataFrame {

	/**
	 * Returns a new date-series where the values have been transformed by the
	 * provided operation.
	 * <p>
	 * The resulting date-series will have the same column names.
	 * 
	 * @param op - the operation used to transform the values
	 * 
	 * @return a new date-series where the values have been transformed by the
	 *         provided operation.
	 */
	DateSeries transform(DoubleUnaryOperator op);

	/**
	 * Joins this date-series with the specifies series, and combines their values
	 * point-wise using the specified operation.
	 * <p>
	 * The resulting date-series will have the same column names as this one.
	 * 
	 * @param series - the series to be joined with this one
	 * @param op     - the operation used to combine the values
	 * 
	 * @return the new joint date-series with value combined using the specified
	 *         operation
	 */
	DateSeries transform(DateSeries series, DoubleBinaryOperator op);

	/**
	 * Returns an immutable {@link Map} view of this date-series.
	 * 
	 * @return an immutable {@code Map} view of this date-series.
	 */
	Map<LocalDate, Double> asMap();
}
