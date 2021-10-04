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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Represents a logical row in a parent {@link DataFrame}, which implements the
 * {@code List<Row>} interface. Rows do not store any data, rather they index
 * into a dataframe which stores the data in {@link Column columns}.
 * <p>
 * Row implements equals and hashCode. Two rows are equals iff they have the
 * same {@link #columnCount()} and the values are equal in each column.
 * 
 * @author biteytech@protonmail.com
 */
public interface Row {

	/**
	 * Returns the row index in the parent {@link DataFrame}, ranging from zero to
	 * dataframe size (exclusive).
	 * 
	 * @return row index in parent dataframe, ranging from zero to dataframe size
	 *         (exclusive).
	 */
	int rowIndex();

	/**
	 * Returns the number of columns in the parent {@link DataFrame}.
	 * 
	 * @return the number of columns in the parent {@code DataFrame}.
	 */
	int columnCount();

	/**
	 * Returns the column name at the specified index.
	 * 
	 * @param columnIndex - the column index
	 * 
	 * @return the column name at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	String columnName(int columnIndex);

	/**
	 * Returns true if the value is null in the specified {@link Column}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return true if the value is null in the specified {@code Column}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	boolean isNull(int columnIndex);

	/**
	 * Returns true if the value is null in the specified {@link Column}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return true if the value is null in the specified {@code Column}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 */
	boolean isNull(String columnName);

	/**
	 * Returns the value for this row in the specified {@link Column}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @param <T>         - the return type. Must be compatible with the column
	 *                    type. No attempt is made to convert between types beyond a
	 *                    cast.
	 * 
	 * @return the value for this row in the specified {@code Column}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column type does not match the
	 *                                   return type.
	 */
	<T extends Comparable<? super T>> T get(int columnIndex);

	/**
	 * Returns the value for this row in the specified {@link Column}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @param <T>        - the return type. Must be compatible with the column type.
	 *                   No attempt is made to convert between types beyond a cast.
	 * 
	 * @return the value for this row in the specified {@code Column}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column type does not match the return
	 *                                  type.
	 */
	<T extends Comparable<? super T>> T get(String columnName);

	/**
	 * {@code boolean} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code BooleanColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a
	 *                                   {@link BooleanColumn}
	 */
	boolean getBoolean(int columnIndex);

	/**
	 * {@code boolean} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code BooleanColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@link BooleanColumn}
	 */
	boolean getBoolean(String columnName);

	/**
	 * {@code int} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code IntColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link IntColumn}
	 */
	int getInt(int columnIndex);

	/**
	 * {@code int} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code IntColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not an {@link IntColumn}
	 */
	int getInt(String columnName);

	/**
	 * Returns the value for this row in the specified {@link IntColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnIndex  - index of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code IntColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an {@link IntColumn}
	 */
	default int getOrDefaultInt(int columnIndex, int defaultValue) {
		return isNull(columnIndex) ? defaultValue : getInt(columnIndex);
	}

	/**
	 * Returns the value for this row in the specified {@link IntColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnName   - name of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code IntColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not an {@link IntColumn}
	 */
	default int getOrDefaultInt(String columnName, int defaultValue) {
		return isNull(columnName) ? defaultValue : getInt(columnName);
	}

	/**
	 * {@code long} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code LongColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link LongColumn}
	 */
	long getLong(int columnIndex);

	/**
	 * {@code long} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code LongColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@link LongColumn}
	 */
	long getLong(String columnName);

	/**
	 * Returns the value for this row in the specified {@link LongColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnIndex  - index of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code LongColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an {@link LongColumn}
	 */
	default long getOrDefaultLong(int columnIndex, long defaultValue) {
		return isNull(columnIndex) ? defaultValue : getLong(columnIndex);
	}

	/**
	 * Returns the value for this row in the specified {@link LongColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnName   - name of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code DoubleColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not an {@link LongColumn}
	 */
	default long getOrDefaultLong(String columnName, long defaultValue) {
		return isNull(columnName) ? defaultValue : getLong(columnName);
	}

	/**
	 * {@code short} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code ShortColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link ShortColumn}
	 */
	short getShort(int columnIndex);

	/**
	 * {@code short} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code ShortColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@link ShortColumn}
	 */
	short getShort(String columnName);

	/**
	 * Returns the value for this row in the specified {@link ShortColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnIndex  - index of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code ShortColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an {@link ShortColumn}
	 */
	default short getOrDefaultShort(int columnIndex, short defaultValue) {
		return isNull(columnIndex) ? defaultValue : getShort(columnIndex);
	}

	/**
	 * Returns the value for this row in the specified {@link ShortColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnName   - name of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code ShortColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not an {@link ShortColumn}
	 */
	default short getOrDefaultShort(String columnName, short defaultValue) {
		return isNull(columnName) ? defaultValue : getShort(columnName);
	}

	/**
	 * {@code double} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DoubleColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link DoubleColumn}
	 */
	double getDouble(int columnIndex);

	/**
	 * {@code double} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DoubleColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@link DoubleColumn}
	 */
	double getDouble(String columnName);

	/**
	 * Returns the value for this row in the specified {@link DoubleColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnIndex  - index of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code DoubleColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an
	 *                                   {@link DoubleColumn}
	 */
	default double getOrDefaultDouble(int columnIndex, double defaultValue) {
		return isNull(columnIndex) ? defaultValue : getDouble(columnIndex);
	}

	/**
	 * Returns the value for this row in the specified {@link DoubleColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnName   - name of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code DoubleColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not an {@link DoubleColumn}
	 */
	default double getOrDefaultDouble(String columnName, double defaultValue) {
		return isNull(columnName) ? defaultValue : getDouble(columnName);
	}

	/**
	 * {@code float} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code FloatColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link FloatColumn}
	 */
	float getFloat(int columnIndex);

	/**
	 * {@code float} primitive specialization of {@link #get(int)}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code FloatColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@link FloatColumn}
	 */
	float getFloat(String columnName);

	/**
	 * Returns the value for this row in the specified {@link FloatColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnIndex  - index of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code FloatColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an {@link FloatColumn}
	 */
	default float getOrDefaultFloat(int columnIndex, float defaultValue) {
		return isNull(columnIndex) ? defaultValue : getFloat(columnIndex);
	}

	/**
	 * Returns the value for this row in the specified {@link FloatColumn}, or the
	 * specified {@code defaultValue} if the value is null.
	 * 
	 * @param columnName   - name of the column in the parent {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for this row in the specified {@code FloatColumn}, or the
	 *         specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not an {@link FloatColumn}
	 */
	default float getOrDefaultFloat(String columnName, float defaultValue) {
		return isNull(columnName) ? defaultValue : getFloat(columnName);
	}

	/**
	 * Returns the {@code yyyymmdd} date for this row in the specified
	 * {@link DateColumn}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the {@code yyyymmdd} date for this row in the specified
	 *         {@code DateColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link DateColumn}
	 */
	int yyyymmdd(int columnIndex);

	/**
	 * Returns the {@code yyyymmdd} date for this row in the specified
	 * {@link DateColumn}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the {@code yyyymmdd} date for this row in the specified
	 *         {@code DateColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@link DateColumn}
	 */
	int yyyymmdd(String columnName);

	/**
	 * Returns the value for this row in the specified {@link StringColumn}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code StringColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code StringColumn}
	 */
	String getString(int columnIndex);

	/**
	 * Returns the value for this row in the specified {@link StringColumn}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code StringColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@code DateColumn}
	 */
	String getString(String columnName);

	/**
	 * Returns the value for this row in the specified {@link DecimalColumn}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DecimalColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a
	 *                                   {@code DecimalColumn}
	 */
	BigDecimal getBigDecimal(int columnIndex);

	/**
	 * Returns the value for this row in the specified {@link DecimalColumn}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DecimalColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@code DecimalColumn}
	 */
	BigDecimal getBigDecimal(String columnName);

	/**
	 * Returns the value for this row in the specified {@link DateColumn}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DateColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code DateColumn}
	 */
	LocalDate getDate(int columnIndex);

	/**
	 * Returns the value for this row in the specified {@link DateColumn}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DateColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a {@code DateColumn}
	 */
	LocalDate getDate(String columnName);

	/**
	 * Returns the value for this row in the specified {@link DateTimeColumn}.
	 * 
	 * @param columnIndex - index of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DateTimeColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a
	 *                                   {@code DateTimeColumn}
	 */
	LocalDateTime getDateTime(int columnIndex);

	/**
	 * Returns the value for this row in the specified {@link DateTimeColumn}.
	 * 
	 * @param columnName - name of the column in the parent {@link DataFrame}.
	 * 
	 * @return the value for this row in the specified {@code DateTimeColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in the parent dataframe.
	 * @throws ClassCastException       if the column is not a
	 *                                  {@code DateTimeColumn}
	 */
	LocalDateTime getDateTime(String columnName);
}
