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

/**
 * A {@link Column} with a numeric element type.
 *
 * @param <E> a {@link Number numeric} type
 * 
 * @author biteytech@protonmail.com
 */
public interface NumericColumn<E extends Number> extends Column<E> {

	/**
	 * Returns the smallest non-null value in this column.
	 * 
	 * @return the smallest value, or NaN if this column is empty or contains only
	 *         null values.
	 */
	double min();

	/**
	 * Returns the largest non-null value in this column.
	 * 
	 * @return the largest value, or NaN if this column is empty or contains only
	 *         null values.
	 */
	double max();

//	// TODO
//	/**
//	 * Returns the median non-null value in this column.
//	 * 
//	 * @return the median value, or NaN if this column is empty or contains only
//	 *         null values.
//	 */
//	double median();

	/**
	 * Returns the mean/average of the non-null values in this column.
	 * 
	 * @return the mean value, or NaN if this column is empty or contains only null
	 *         values.
	 */
	double mean();

	/**
	 * Returns the specified standard deviation of the non-null values in this
	 * column.
	 * 
	 * @param population - computes population stddev (over {@code n}) if true,
	 *                   otherwise computes sample stddev (over {@code n - 1}).
	 * 
	 * 
	 * @return the specified stddev value, or NaN if this column is empty or
	 *         contains only null values.
	 */
	double stddev(boolean population);

	/**
	 * Returns the sample standard deviation of the non-null values in this column.
	 * 
	 * @return the sample stddev value, or NaN if this column is empty or contains
	 *         only null values.
	 */
	default double stddev() {
		return stddev(false);
	}
}
