/*
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

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public interface DataFrame extends List<Row>, RandomAccess {

//	static final long MAGIC_NUMBER = ((long) 'd') << 56 | ((long) 'a') << 48 | ((long) 't') << 40 | ((long) 'a') << 32
//			| 'f' << 24 | 'r' << 16 | 'a' << 8 | 'm';

	/*--------------------------------------------------------------------------------
	 *	Object, Collection, and List style Methods
	 *--------------------------------------------------------------------------------*/
	/**
	 * Tests this DataFrame against the specified one for equality. Only compares
	 * the column data if {@code dataOnly} is true, otherwise compares column names
	 * and {@link #keyColumnIndex()} as well.
	 * 
	 * @param df       - the dataframe to compare against
	 * @param dataOnly - only compares column data if true, otherwise compares
	 *                 column names and {@link #keyColumnIndex()} as well
	 * 
	 * @return true if all column comparisons are true, and either {@code dataOnly}
	 *         is true or the column names and {@link #keyColumnIndex()} are the
	 *         same as well.
	 */
	boolean equals(DataFrame df, boolean dataOnly);

	/**
	 * Returns a deep copy of this dataframe. All column data will be copied into
	 * newly allocated buffers.
	 * 
	 * @return a deep copy of this dataframe.
	 */
	DataFrame copy();

	/*--------------------------------------------------------------------------------
	 *	Miscellaneous Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	default Spliterator<Row> spliterator() {
		return Spliterators.spliterator(this, ORDERED | IMMUTABLE | NONNULL);
	}

	/**
	 * Returns a {@link Cursor} at the specified row index.
	 * 
	 * @param rowIndex - the row index
	 * 
	 * @return a {@code Cursor} at the specified row index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 */
	Cursor cursor(int rowIndex);

	/**
	 * Returns a {@link Cursor} pointing at the first row in this dataframe.
	 * 
	 * @return a {@code Cursor} pointing at the first row in this dataframe.
	 * 
	 * @throws IndexOutOfBoundsException if this dataframe is empty
	 */
	default Cursor cursor() {
		return cursor(0);
	}

	/**
	 * Returns a map keyed by the elements in the key column, with values from the
	 * specified index.
	 * 
	 * @param <K>         - the key type. Must match the key column type.
	 * @param <V>         - the value type. Must match the specified column type.
	 * 
	 * @param columnIndex - index of a column in this dataframe
	 * 
	 * @return a map keyed by the elements in the key column, with values from the
	 *         specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	<K, V> Map<K, V> toMap(int columnIndex);

	/**
	 * Returns a map keyed by the elements in the key column, with values from the
	 * specified index.
	 * 
	 * @param <K>        - the key type. Must match the key column type.
	 * @param <V>        - the value type. Must match the specified column type.
	 * 
	 * @param columnName - name of a column in this dataframe
	 * 
	 * @return a map keyed by the elements in the key column, with values from the
	 *         specified index.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	<K, V> Map<K, V> toMap(String columnName);

	/*--------------------------------------------------------------------------------
	 *	Key Column Methods
	 *--------------------------------------------------------------------------------*/
	/**
	 * Returns true if this dataframe has a key column.
	 * 
	 * @return true if this dataframe has a key column.
	 */
	boolean hasKeyColumn();

	/**
	 * Returns the index of the key column, or null if no key column has been
	 * specified.
	 * 
	 * @return the index of the key column, or null if no key column has been
	 *         specified.
	 */
	Integer keyColumnIndex();

	/**
	 * Returns the key column name, or null if no key column has been specified.
	 * 
	 * @return the key column name, or null if no key column has been specified.
	 */
	String keyColumnName();

	/**
	 * Returns the key {@link ColumnType column type}, or null if no key column has
	 * been specified.
	 * 
	 * @return the key column type, or null if no key column has been specified.
	 */
	ColumnType keyColumnType();

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but with the key
	 * column set to the specified index.
	 * 
	 * @param columnIndex - the key column index
	 * 
	 * @return a new dataframe with the key column set to the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	DataFrame withKeyColumn(int columnIndex);

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but with the key
	 * column set to the specified index.
	 * 
	 * @param columnName - name of the column in this dataframe
	 * 
	 * @return a new dataframe with the key column set to the specified index.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	DataFrame withKeyColumn(String columnName);

	/*--------------------------------------------------------------------------------
	 *	Column Methods
	 *--------------------------------------------------------------------------------*/
	/**
	 * Returns the number of columns in this dataframe.
	 * 
	 * @return the number of columns in this dataframe.
	 */
	int columnCount();

	/**
	 * Returns the index of the specified column.
	 * 
	 * @param columnName - name of the column in this dataframe
	 * 
	 * @return the index of the specified column.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	int columnIndex(String columnName);

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
	 * Returns the {@link ColumnType column type} at the specified index.
	 * 
	 * @param columnIndex - the column index
	 * 
	 * @return the column type at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	ColumnType columnType(int columnIndex);

	/**
	 * Returns the {@link ColumnType column type} at the specified index.
	 * 
	 * @param columnName - name of the column in this dataframe
	 * 
	 * @return the column type at the specified index.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	ColumnType columnType(String columnName);

	/**
	 * Returns an ordered map where the entries are (name, column) pairs.
	 * 
	 * @return an ordered map where the entries are (name, column) pairs.
	 */
	LinkedHashMap<String, Column<?>> columnMap();

	/**
	 * Returns a list of columns in this dataframe.
	 * 
	 * @return a list of columns in this dataframe.
	 */
	List<Column<?>> columns();

	/**
	 * Returns a list of column names in this dataframe.
	 * 
	 * @return a list of column names in this dataframe.
	 */
	List<String> columnNames();

	/**
	 * Returns a list of column types in this dataframe.
	 * 
	 * @return a list of column types in this dataframe.
	 */
	List<ColumnType> columnTypes();

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but including
	 * the specified column and column name. If a column already exists with the
	 * same name, it will be replaced.
	 * 
	 * @param columnName - name of the column to be added or replaced
	 * @param column     - the column
	 * 
	 * @return a new dataframe with the specified column and column name
	 */
	DataFrame withColumn(String columnName, Column<?> column);

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but including
	 * the specified columns and column names. If a column already exists with one
	 * of the specified names, it will be replaced.
	 * 
	 * @param columnNames - names of the columns to be added or replaced
	 * @param columns     - the columns
	 * 
	 * @return a new dataframe with the specified columns and column names
	 * 
	 * @throws IllegalArgumentException if the two input arrays have different
	 *                                  lengths
	 */
	DataFrame withColumns(String[] columnNames, Column<?>[] columns);

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but including
	 * the specified columns and column names. If a column already exists with one
	 * of the specified names, it will be replaced.
	 * 
	 * @param columns - an ordered list of entries containing columns and column
	 *                names
	 * 
	 * @return a new dataframe with the specified columns and column names
	 */
	DataFrame withColumns(LinkedHashMap<String, Column<?>> columns);

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but including
	 * the columns and column names from the provided dataframe. If a column already
	 * exists with one of the provided names, it will be replaced.
	 * 
	 * @param df - a dataframe whose columns will be included in the result
	 * 
	 * @return a new dataframe with the specified columns and column names
	 */
	DataFrame withColumns(DataFrame df);

	/**
	 * Returns a new dataframe which contains only the specified columns, in the
	 * specified order.
	 * 
	 * @param columnNames - the columns names to be included in the result
	 * 
	 * @return a new dataframe which contains only the specified columns, in the
	 *         specified order.
	 */
	DataFrame selectColumns(List<String> columnNames);

	/**
	 * Returns a new dataframe which contains only the specified columns, in the
	 * specified order.
	 * 
	 * @param columnNames - the columns names to be included in the result
	 * 
	 * @return a new dataframe which contains only the specified columns, in the
	 *         specified order.
	 */
	DataFrame selectColumns(String... columnNames);

	/**
	 * Returns a new dataframe which contains only the specified columns, in the
	 * specified order.
	 * 
	 * @param columnIndices - the columns to be included in the result
	 * 
	 * @return a new dataframe which contains only the specified columns, in the
	 *         specified order.
	 */
	DataFrame selectColumns(int... columnIndices);

	/**
	 * Returns a new dataframe which excludes the specified columns.
	 * 
	 * @param columnNames - the columns names to be excluded from the result
	 * 
	 * @return a new dataframe which excludes the specified columns.
	 */
	DataFrame dropColumns(Collection<String> columnNames);

	/**
	 * Returns a new dataframe which excludes the specified columns.
	 * 
	 * @param columnNames - the columns names to be excluded from the result
	 * 
	 * @return a new dataframe which excludes the specified columns.
	 */
	DataFrame dropColumns(String... columnNames);

	/**
	 * Returns a new dataframe which excludes the specified columns.
	 * 
	 * @param columnIndices - the columns to be excluded from the result
	 * 
	 * @return a new dataframe which excludes the specified columns.
	 */
	DataFrame dropColumns(int... columnIndices);

	/**
	 * Returns the {@link Column} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @param <T>         - the return type. Must be compatible with the column
	 *                    type. No attempt is made to convert between types beyond a
	 *                    cast.
	 * 
	 * @return the column at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column type does not match the
	 *                                   return type.
	 */
	<T> Column<T> column(int columnIndex);

	/**
	 * Returns the {@link StringColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code StringColumn}
	 */
	StringColumn stringColumn(int columnIndex);

	/**
	 * Returns the {@link BooleanColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a
	 *                                   {@code BooleanColumn}
	 */
	BooleanColumn booleanColumn(int columnIndex);

	/**
	 * Returns the {@link IntColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code IntColumn}
	 */
	IntColumn intColumn(int columnIndex);

	/**
	 * Returns the {@link LongColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code LongColumn}
	 */
	LongColumn longColumn(int columnIndex);

	/**
	 * Returns the {@link DoubleColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code DoubleColumn}
	 */
	DoubleColumn doubleColumn(int columnIndex);

	/**
	 * Returns the {@link FloatColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code FloatColumn}
	 */
	FloatColumn floatColumn(int columnIndex);

	/**
	 * Returns the {@link DateColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code DateColumn}
	 */
	DateColumn dateColumn(int columnIndex);

	/**
	 * Returns the {@link DateTimeColumn} at the specified index.
	 * 
	 * @param columnIndex - index of the column
	 * 
	 * @return the column at the specified index
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a
	 *                                   {@code DateTimeColumn}
	 */
	DateTimeColumn dateTimeColumn(int columnIndex);

	/**
	 * Returns the specified {@link Column}
	 * 
	 * @param columnName - column name
	 * 
	 * @param <T>        - the return type. Must be compatible with the column type.
	 *                   No attempt is made to convert between types beyond a cast.
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column type does not match the return
	 *                                  type.
	 */
	<T> Column<T> column(String columnName);

	/**
	 * Returns the specified {@link StringColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@code StringColumn}
	 */
	StringColumn stringColumn(String columnName);

	/**
	 * Returns the specified {@link BooleanColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@code BooleanColumn}
	 */
	BooleanColumn booleanColumn(String columnName);

	/**
	 * Returns the specified {@link IntColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@code IntColumn}
	 */
	IntColumn intColumn(String columnName);

	/**
	 * Returns the specified {@link LongColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@code LongColumn}
	 */
	LongColumn longColumn(String columnName);

	/**
	 * Returns the specified {@link DoubleColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@code DoubleColumn}
	 */
	DoubleColumn doubleColumn(String columnName);

	/**
	 * Returns the specified {@link FloatColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@code FloatColumn}
	 */
	FloatColumn floatColumn(String columnName);

	/**
	 * Returns the specified {@link DateColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@code DateColumn}
	 */
	DateColumn dateColumn(String columnName);

	/**
	 * Returns the specified {@link DateTimeColumn}
	 * 
	 * @param columnName - column name
	 * 
	 * @return the specified column
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a
	 *                                  {@code DateTimeColumn}
	 */
	DateTimeColumn dateTimeColumn(String columnName);

	/**
	 * Derive a new {@link Column} from the rows of this dataframe. The new column
	 * will have the same size as this dataframe, and each element will have been
	 * derived from the corresponding row.
	 * 
	 * @param type     - the new column's {@link ColumnType type}
	 * @param function - the function used to compute column elements from dataframe
	 *                 rows
	 * 
	 * @param <T>      - the return type. Must be compatible with the column type.
	 *                 No attempt is made to convert between types beyond a cast.
	 * 
	 * @return the derived column
	 * 
	 * @throws ClassCastException if the column type does not match the return type.
	 */
	<T> Column<T> deriveColumn(ColumnType type, Function<Row, T> function);

	/**
	 * Derive a new {@link IntColumn} from the rows of this dataframe. The new
	 * column will have the same size as this dataframe, and each element will have
	 * been derived from the corresponding row.
	 * 
	 * @param function - the function used to compute column elements from dataframe
	 *                 rows
	 * 
	 * @return the derived column
	 */
	IntColumn deriveColumn(ToIntFunction<Row> function);

	/**
	 * Derive a new {@link LongColumn} from the rows of this dataframe. The new
	 * column will have the same size as this dataframe, and each element will have
	 * been derived from the corresponding row.
	 * 
	 * @param function - the function used to compute column elements from dataframe
	 *                 rows
	 * 
	 * @return the derived column
	 */
	LongColumn deriveColumn(ToLongFunction<Row> function);

	/**
	 * Derive a new {@link DoubleColumn} from the rows of this dataframe. The new
	 * column will have the same size as this dataframe, and each element will have
	 * been derived from the corresponding row.
	 * 
	 * @param function - the function used to compute column elements from dataframe
	 *                 rows
	 * 
	 * @return the derived column
	 */
	DoubleColumn deriveColumn(ToDoubleFunction<Row> function);

	/**
	 * Derive a new {@link FloatColumn} from the rows of this dataframe. The new
	 * column will have the same size as this dataframe, and each element will have
	 * been derived from the corresponding row.
	 * 
	 * @param function - the function used to compute column elements from dataframe
	 *                 rows
	 * 
	 * @return the derived column
	 */
	FloatColumn deriveColumn(ToFloatFunction<Row> function);

	/**
	 * Derive a new {@link BooleanColumn} from the rows of this dataframe. The new
	 * column will have the same size as this dataframe, and each element will have
	 * been derived from the corresponding row.
	 * 
	 * @param function - the function used to compute column elements from dataframe
	 *                 rows
	 * 
	 * @return the derived column
	 */
	BooleanColumn deriveColumn(Predicate<Row> function);

	/*--------------------------------------------------------------------------------
	 *	Row Selection Methods
	 *--------------------------------------------------------------------------------*/

	/**
	 * Returns a dataframe containing {@code sampleSize} rows selected at random.
	 * The rows in the resulting dataframe will be in the same order as in this one.
	 * 
	 * @param sampleSize - the number of rows to sample from this dataframe
	 * 
	 * @return a dataframe containing {@code sampleSize} rows selected at random.
	 * 
	 * @throws IndexOutOfBoundsException if {@code sampleSize} is negative or is
	 *                                   greater than {@code #size()}
	 */
	DataFrame sampleN(int sampleSize);

	/**
	 * Returns a dataframe containing {@code size()*proportion} rows selected at
	 * random. The rows in the resulting dataframe will be in the same order as in
	 * this one.
	 * 
	 * @param proportion - a number between 0.0 and 1.0 inclusive
	 * 
	 * @return a dataframe containing {@code size()*proportion} rows selected at
	 *         random.
	 */
	DataFrame sampleX(double proportion);

	/**
	 * Returns a dataframe containing the first {@code count} rows from this
	 * dataframe, or this dataframe if it contains fewer than {@code count} rows.
	 * 
	 * @param count - number of rows to return from the "top" or "head" of this
	 *              dataframe.
	 * 
	 * @return a dataframe containing the first {@code count} rows from this
	 *         dataframe.
	 */
	DataFrame head(int count);

	/**
	 * Returns an empty dataframe. The resulting dataframe will have the same
	 * meta-data (column names, key column index) as this dataframe, but all of the
	 * columns will be empty.
	 * 
	 * @return an empty dataframe with the same meta-data as this dataframe.
	 */
	default DataFrame empty() {
		return head(0);
	}

	/**
	 * Returns a dataframe containing the first 10 rows from this dataframe, or this
	 * dataframe if it contains fewer than 10 rows.
	 * 
	 * @return a dataframe containing the first 10 rows from this dataframe.
	 */
	default DataFrame head() {
		return head(10);
	}

	/**
	 * Returns a dataframe containing the last {@code count} rows from this
	 * dataframe, or this dataframe if it contains fewer than {@code count} rows.
	 * 
	 * @param count - number of rows to return from the "end" or "tail" of this
	 *              dataframe.
	 * 
	 * @return a dataframe containing the last {@code count} rows from this
	 *         dataframe.
	 */
	DataFrame tail(int count);

	/**
	 * Returns a dataframe containing the last 10 rows from this dataframe, or this
	 * dataframe if it contains fewer than 10 rows.
	 * 
	 * @return a dataframe containing the last 10 rows from this dataframe.
	 */
	default DataFrame tail() {
		return tail(10);
	}

	/**
	 * Returns a dataframe containing the rows between the specified
	 * <tt>fromIndex</tt>, inclusive, and <tt>toIndex</tt>, exclusive (if
	 * <tt>fromIndex</tt> and <tt>toIndex</tt> are equal, the returned dataframe is
	 * empty).
	 * 
	 * @param fromIndex - index of the lowest row (inclusive)
	 * @param toIndex   - index of the highest row (exclusive)
	 * 
	 * @return a dataframe containing the rows between the specified
	 *         <tt>fromIndex</tt>, inclusive, and <tt>toIndex</tt>, exclusive
	 * 
	 * @throws IndexOutOfBoundsException if either index is negative or is greater
	 *                                   than {@link #size()}, or if {@code toIndex}
	 *                                   is less than {@code fromIndex}
	 */
	DataFrame subFrame(int fromIndex, int toIndex);

	/**
	 * Returns the rows from this dataframe whose key column values are strictly
	 * less than {@code toKey}.
	 *
	 * @param toKey high endpoint (exclusive) of the key column values in the
	 *              returned dataframe
	 * 
	 * @return the rows from this dataframe whose key column values are strictly
	 *         less than {@code toKey}.
	 * 
	 * @throws UnsupportedOperationException if this dataframe does not contain a
	 *                                       key column
	 * @throws ClassCastException            if {@code toKey} is not compatible with
	 *                                       this dataframe's key column type
	 * @throws NullPointerException          if {@code toKey} is null
	 */
	DataFrame headTo(Object toKey);

	/**
	 * Returns the rows from this dataframe whose key column values are greater than
	 * or equal to {@code fromKey}.
	 *
	 * @param fromKey low endpoint (inclusive) of the key column values in the
	 *                returned dataframe
	 * 
	 * @return the rows from this dataframe whose key column values are greater than
	 *         or equal to {@code fromKey}.
	 * 
	 * @throws UnsupportedOperationException if this dataframe does not contain a
	 *                                       key column
	 * @throws ClassCastException            if {@code fromKey} is not compatible
	 *                                       with this dataframe's key column type
	 * @throws NullPointerException          if {@code fromKey} is null
	 */
	DataFrame tailFrom(Object fromKey);

	/**
	 * Returns the rows from this dataframe whose key column values are greater than
	 * or equal to {@code fromKey} and strictly less than {@code toKey}.
	 *
	 * @param fromKey low endpoint (inclusive) of the key column values in the
	 *                returned dataframe
	 * @param toKey   high endpoint (exclusive) of the key column values in the
	 *                returned dataframe
	 * 
	 * @return the rows from this dataframe whose key column values are greater than
	 *         or equal to {@code fromKey} and strictly less than {@code toKey}.
	 * 
	 * @throws UnsupportedOperationException if this dataframe does not contain a
	 *                                       key column
	 * @throws ClassCastException            if either key is not compatible with
	 *                                       this dataframe's key column type
	 * @throws NullPointerException          if either key is null
	 */
	DataFrame subFrameByValue(Object fromKey, Object toKey);

	/**
	 * Returns a dataframe containing the rows which pass the specified
	 * {@link Predicate}.
	 * 
	 * @param criteria - the {@code Predicate} used to the the rows in this
	 *                 dataframe
	 * 
	 * @return a dataframe containing the rows which pass the specified
	 *         {@code Predicate}.
	 */
	DataFrame filter(Predicate<Row> criteria);

	/*--------------------------------------------------------------------------------
	 *	Database-like Methods
	 *--------------------------------------------------------------------------------*/
	/**
	 * Appends the specified dataframe to this one by calling
	 * {@link Column#append(Column)} on each pair of columns, and keeping the
	 * meta-data from this dataframe.
	 * 
	 * @param df - the dataframe to be appended to this one
	 * 
	 * @return a new dataframe resulting from appended the specified dataframe to
	 *         this one.
	 * 
	 * @throws IllegalArgumentException if the column counts or types do not match
	 *                                  between the two dataframes
	 */
	DataFrame append(DataFrame df); // union all

	/**
	 * Appends the specified dataframe to this one by calling
	 * {@link Column#append(Column,boolean)} on each pair of columns, and keeping
	 * the meta-data from this dataframe.
	 * 
	 * @param df     - the dataframe to be appended to this one
	 * @param coerce - specifies if a sole sorted column should be converted to a
	 *               heap
	 * 
	 * @return a new dataframe resulting from appended the specified dataframe to
	 *         this one.
	 * 
	 * @throws IllegalArgumentException if the column counts or types do not match
	 *                                  between the two dataframes
	 */
	DataFrame append(DataFrame df, boolean coerce);

	/**
	 * Perform a one-to-one inner join on this (left) dataframe with the specified
	 * (right) dataframe by their key columns. The resulting dataframe will have a
	 * key column which is the intersection of the left and right key columns.
	 * <p>
	 * If the left dataframe has {@code N} columns and the right has {@code M}
	 * columns then the resulting dataframe will have {@code N + M - 1} columns,
	 * starting with all N columns from the left dataframe, followed by the columns
	 * from the right dataframe excluding the right key column. If the right
	 * dataframe has any column names in common with the left, the duplicate right
	 * column names will have a suffix appended to them in the result.
	 * <p>
	 * If the left dataframe has {@code S} rows and the right has {@code T} rows
	 * then this join operation will use {@code O(S + T)} space and time. The space
	 * overhead is approximately {@code S + T} bits. The result will contain at most
	 * {@code min(S, T)} rows, and will be empty if the two key columns do not have
	 * any elements in common.
	 * 
	 * @param df - the right dataframe to be joined with this left one
	 * 
	 * @return a new dataframe formed by the intersection of the left and right key
	 *         columns.
	 * 
	 * @throws IllegalArgumentException if either dataframe does not have a key
	 *                                  column, or if the key columns do not have
	 *                                  the same type.
	 */
	DataFrame join(DataFrame df);

	/**
	 * Perform a one-to-many inner join on this (left) dataframe with the specified
	 * (right) dataframe using the key column from the left dataframe. Elements from
	 * the specified column in the right dataframe will be matched against the key
	 * column using a binary search.
	 * <p>
	 * If the left dataframe has {@code N} columns and the right has {@code M}
	 * columns then the resulting dataframe will have {@code N + M - 1} columns,
	 * starting with all N columns from the left dataframe, followed by the columns
	 * from the right dataframe excluding the specified non-index column. If the
	 * right dataframe has any column names in common with the left, the duplicate
	 * right column names will have a suffix appended to them in the result.
	 * <p>
	 * If the left dataframe has {@code S} rows and the right has {@code T} rows
	 * then this join operation will use {@code O(T)} space and run in
	 * {@code O(T*log(S))} time. The space overhead is approximately
	 * {@code T*(4 bytes + 1 bit)}.
	 * 
	 * @param df              - the right dataframe to be joined with this left one
	 * @param rightColumnName - name of the column in the right dataframe which will
	 *                        be matched against the key column from the left
	 *                        dataframe
	 * 
	 * @return a new dataframe formed by the one-to-many inner join of this
	 *         dataframe with the specified dataframe on the specified column
	 * 
	 * @throws IllegalArgumentException if this dataframe does not have a key
	 *                                  column, or the specified column name is not
	 *                                  recognized in the right dataframe, or the
	 *                                  key column and specified column do not have
	 *                                  the same type.
	 */
	DataFrame joinOneToMany(DataFrame df, String rightColumnName);

	/**
	 * Perform a many-to-one inner join on this (left) dataframe with the specified
	 * (right) dataframe using the key column from the right dataframe. Elements
	 * from the specified column in the left dataframe will be matched against the
	 * key column using a binary search.
	 * <p>
	 * If the left dataframe has {@code N} columns and the right has {@code M}
	 * columns then the resulting dataframe will have {@code N + M - 1} columns,
	 * starting with all N columns from the left dataframe, followed by the columns
	 * from the right dataframe excluding the key column. If the right dataframe has
	 * any column names in common with the left, the duplicate right column names
	 * will have a suffix appended to them in the result.
	 * <p>
	 * If the left dataframe has {@code S} rows and the right has {@code T} rows
	 * then this join operation will use {@code O(S)} space and run in
	 * {@code O(S*log(T))} time. The space overhead is approximately
	 * {@code S*(4 bytes + 1 bit)}.
	 * 
	 * @param df             - the right dataframe to be joined with this left one
	 * @param leftColumnName - name of the column in the left dataframe which will
	 *                       be matched against the key column from the right
	 *                       dataframe
	 * 
	 * @return a new dataframe formed by the many-to-one inner join of this
	 *         dataframe with the specified dataframe on the specified column
	 * 
	 * @throws IllegalArgumentException if the specified dataframe does not have a
	 *                                  key column, or the specified column name is
	 *                                  not recognized in this dataframe, or the key
	 *                                  column and specified column do not have the
	 *                                  same type.
	 */
	DataFrame joinManyToOne(DataFrame df, String leftColumnName);

	/**
	 * Perform a one-to-many left join on this (left) dataframe with the specified
	 * (right) dataframe using the key column from the left dataframe. Elements from
	 * the specified column in the right dataframe will be matched against the key
	 * column using a binary search. Any unmatched rows from this dataframe will
	 * appear in the resulting dataframe with {@code null} values filled in for the
	 * columns from the right dataframe.
	 * <p>
	 * If the left dataframe has {@code N} columns and the right has {@code M}
	 * columns then the resulting dataframe will have {@code N + M - 1} columns,
	 * starting with all N columns from the left dataframe, followed by the columns
	 * from the right dataframe excluding the specified non-index column. If the
	 * right dataframe has any column names in common with the left, the duplicate
	 * right column names will have a suffix appended to them in the result.
	 * <p>
	 * If the left dataframe has {@code S} rows and the right has {@code T} rows
	 * then this join operation will use {@code O(S + T)} space and run in
	 * {@code O(T*log(S))} time. The space overhead is approximately S bits, plus
	 * {@code T*(4 bytes + 1 bit)}.
	 * 
	 * @param df              - the right dataframe to be joined with this left one
	 * @param rightColumnName - name of the column in the right dataframe which will
	 *                        be matched against the key column from the left
	 *                        dataframe
	 * 
	 * @return a new dataframe formed by the one-to-many left join of this dataframe
	 *         with the specified dataframe on the specified column
	 * 
	 * @throws IllegalArgumentException if this dataframe does not have a key
	 *                                  column, or the specified column name is not
	 *                                  recognized in the right dataframe, or the
	 *                                  key column and specified column do not have
	 *                                  the same type.
	 */
	DataFrame joinLeftOneToMany(DataFrame df, String rightColumnName);

	/**
	 * Perform a one-to-many inner join on this (left) dataframe with the specified
	 * (right) dataframe by building a hashtable index on the specified left
	 * columns. The left columns taken together must form a unique index.
	 * <p>
	 * If the left dataframe has {@code N} columns, the right has {@code M} columns,
	 * and the hashtable index has {@code H} columns then the resulting dataframe
	 * will have {@code N + M - H} columns, starting with all N columns from the
	 * left dataframe, followed by the columns from the right dataframe excluding
	 * the hashtable index columns. If the right dataframe has any column names in
	 * common with the left, the duplicate right column names will have a suffix
	 * appended to them in the result.
	 * <p>
	 * If the left dataframe has {@code S} rows and the right has {@code T} rows
	 * then this join operation will use {@code O(S + T)} space and time. The space
	 * overhead is approximately T bits, plus the space required by an
	 * {@code <Integer,Integer>} {@link java.util.HashMap} with {@code S} entries,
	 * plus an additional {@code S*8} bytes.
	 * 
	 * @param df               - the right dataframe to be joined with this left one
	 * @param leftColumnNames  - names of columns in this (left) dataframe, which
	 *                         taken together form a unique index
	 * @param rightColumnNames - corresponding columns in the specified (right)
	 *                         dataframe
	 * 
	 * @return a new dataframe formed by the one-to-many inner join of this
	 *         dataframe with the specified dataframe on the specified columns
	 * 
	 * @throws IllegalStateException    if the left columns taken together do not
	 *                                  form a unique index
	 * @throws IllegalArgumentException if either list of column names is empty, if
	 *                                  the two lists do not have the same length,
	 *                                  if the respective columns do not have the
	 *                                  same types, or if any of the column names
	 *                                  are not recognized.
	 */
	DataFrame join(DataFrame df, String[] leftColumnNames, String[] rightColumnNames);

	/*--------------------------------------------------------------------------------
	 *	Cell Accessors
	 *--------------------------------------------------------------------------------*/
	/**
	 * Returns true if the value is null in the specified {@link Column} at the
	 * specified row index.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return true if the value is null in the specified {@code Column} at the
	 *         specified row index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	boolean isNull(int rowIndex, int columnIndex);

	/**
	 * Returns true if the value is null in the specified {@link Column} at the
	 * specified row index.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return true if the value is null in the specified {@code Column} at the
	 *         specified row index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 */
	boolean isNull(int rowIndex, String columnName);

	/**
	 * Returns the value for the specified row in the specified {@link Column}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @param <T>         - the return type. Must be compatible with the column
	 *                    type. No attempt is made to convert between types beyond a
	 *                    cast.
	 * 
	 * @return the value for the specified row in the specified {@code Column}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column type does not match the
	 *                                   return type.
	 */
	<T> T get(int rowIndex, int columnIndex);

	/**
	 * Returns the value for the specified row in the specified {@link Column}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @param <T>        - the return type. Must be compatible with the column type.
	 *                   No attempt is made to convert between types beyond a cast.
	 * 
	 * @return the value for the specified row in the specified {@code Column}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column type does not match the
	 *                                   return type.
	 */
	<T> T get(int rowIndex, String columnName);

	/**
	 * Returns the value for the specified row in the specified
	 * {@link StringColumn}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code StringColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code StringColumn}
	 */
	String getString(int rowIndex, int columnIndex);

	/**
	 * Returns the value for the specified row in the specified
	 * {@link StringColumn}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code StringColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not a {@code DateColumn}
	 */
	String getString(int rowIndex, String columnName);

	/**
	 * {@code boolean} primitive specialization of {@link #get(int,int)}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code BooleanColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a
	 *                                   {@link BooleanColumn}
	 */
	boolean getBoolean(int rowIndex, int columnIndex);

	/**
	 * {@code boolean} primitive specialization of {@link #get(int,String)}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code BooleanColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not a
	 *                                   {@link BooleanColumn}
	 */
	boolean getBoolean(int rowIndex, String columnName);

	/**
	 * {@code int} primitive specialization of {@link #get(int,int)}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code IntColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link IntColumn}
	 */
	int getInt(int rowIndex, int columnIndex);

	/**
	 * {@code int} primitive specialization of {@link #get(int,String)}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code IntColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not an {@link IntColumn}
	 */
	int getInt(int rowIndex, String columnName);

	/**
	 * Returns the value for the specified row in the specified {@link IntColumn},
	 * or the specified {@code defaultValue} if the value is null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnIndex  - index of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified {@code IntColumn},
	 *         or the specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an {@link IntColumn}
	 */
	default int getOrDefaultInt(int rowIndex, int columnIndex, int defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getInt(rowIndex, columnIndex);
	}

	/**
	 * Returns the value for the specified row in the specified {@link IntColumn},
	 * or the specified {@code defaultValue} if the value is null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnName   - name of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified {@code IntColumn},
	 *         or the specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not an {@link IntColumn}
	 */
	default int getOrDefaultInt(int rowIndex, String columnName, int defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getInt(rowIndex, columnName);
	}

	/**
	 * {@code long} primitive specialization of {@link #get(int,int)}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code LongColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link LongColumn}
	 */
	long getLong(int rowIndex, int columnIndex);

	/**
	 * {@code long} primitive specialization of {@link #get(int,String)}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code LongColumn}.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not a {@link LongColumn}
	 */
	long getLong(int rowIndex, String columnName);

	/**
	 * Returns the value for the specified row in the specified {@link LongColumn},
	 * or the specified {@code defaultValue} if the value is null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnIndex  - index of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified {@code LongColumn},
	 *         or the specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an {@link LongColumn}
	 */
	default long getOrDefaultLong(int rowIndex, int columnIndex, long defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getLong(rowIndex, columnIndex);
	}

	/**
	 * Returns the value for the specified row in the specified {@link LongColumn},
	 * or the specified {@code defaultValue} if the value is null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnName   - name of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code DoubleColumn}, or the specified {@code defaultValue} if the
	 *         value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not an {@link LongColumn}
	 */
	default long getOrDefaultLong(int rowIndex, String columnName, long defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getLong(rowIndex, columnName);
	}

	/**
	 * {@code double} primitive specialization of {@link #get(int,int)}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code DoubleColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link DoubleColumn}
	 */
	double getDouble(int rowIndex, int columnIndex);

	/**
	 * {@code double} primitive specialization of {@link #get(int,String)}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code DoubleColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not a {@link DoubleColumn}
	 */
	double getDouble(int rowIndex, String columnName);

	/**
	 * Returns the value for the specified row in the specified
	 * {@link DoubleColumn}, or the specified {@code defaultValue} if the value is
	 * null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnIndex  - index of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code DoubleColumn}, or the specified {@code defaultValue} if the
	 *         value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an
	 *                                   {@link DoubleColumn}
	 */
	default double getOrDefaultDouble(int rowIndex, int columnIndex, double defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getDouble(rowIndex, columnIndex);
	}

	/**
	 * Returns the value for the specified row in the specified
	 * {@link DoubleColumn}, or the specified {@code defaultValue} if the value is
	 * null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnName   - name of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code DoubleColumn}, or the specified {@code defaultValue} if the
	 *         value is null.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe.
	 * @throws ClassCastException       if the column is not an {@link DoubleColumn}
	 */
	default double getOrDefaultDouble(int rowIndex, String columnName, double defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getDouble(rowIndex, columnName);
	}

	/**
	 * {@code float} primitive specialization of {@link #get(int,int)}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code FloatColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link FloatColumn}
	 */
	float getFloat(int rowIndex, int columnIndex);

	/**
	 * {@code float} primitive specialization of {@link #get(int,String)}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code FloatColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not a {@link FloatColumn}
	 */
	float getFloat(int rowIndex, String columnName);

	/**
	 * Returns the value for the specified row in the specified {@link FloatColumn},
	 * or the specified {@code defaultValue} if the value is null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnIndex  - index of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified {@code FloatColumn},
	 *         or the specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not an {@link FloatColumn}
	 */
	default float getOrDefaultFloat(int rowIndex, int columnIndex, float defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getFloat(rowIndex, columnIndex);
	}

	/**
	 * Returns the value for the specified row in the specified {@link FloatColumn},
	 * or the specified {@code defaultValue} if the value is null.
	 * 
	 * @param rowIndex     - the row index
	 * @param columnName   - name of the column in this {@link DataFrame}.
	 * @param defaultValue - the value to return instead of null
	 * 
	 * @return the value for the specified row in the specified {@code FloatColumn},
	 *         or the specified {@code defaultValue} if the value is null.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not an {@link FloatColumn}
	 */
	default float getOrDefaultFloat(int rowIndex, String columnName, float defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getFloat(rowIndex, columnName);
	}

	/**
	 * Returns the value for the specified row in the specified {@link DateColumn}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code DateColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@code DateColumn}
	 */
	LocalDate getDate(int rowIndex, int columnIndex);

	/**
	 * Returns the value for the specified row in the specified {@link DateColumn}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified {@code DateColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not a {@code DateColumn}
	 */
	LocalDate getDate(int rowIndex, String columnName);

	/**
	 * Returns the {@code yyyymmdd} date for the specified row in the specified
	 * {@link DateColumn}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the {@code yyyymmdd} date for the specified row in the specified
	 *         {@code DateColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a {@link DateColumn}
	 */
	int yyyymmdd(int rowIndex, int columnIndex);

	/**
	 * Returns the {@code yyyymmdd} date for the specified row in the specified
	 * {@link DateColumn}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the {@code yyyymmdd} date for the specified row in the specified
	 *         {@code DateColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not a {@link DateColumn}
	 */
	int yyyymmdd(int rowIndex, String columnName);

	/**
	 * Returns the value for the specified row in the specified
	 * {@link DateTimeColumn}.
	 * 
	 * @param rowIndex    - the row index
	 * @param columnIndex - index of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code DateTimeColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 * @throws ClassCastException        if the column is not a
	 *                                   {@code DateTimeColumn}
	 */
	LocalDateTime getDateTime(int rowIndex, int columnIndex);

	/**
	 * Returns the value for the specified row in the specified
	 * {@link DateTimeColumn}.
	 * 
	 * @param rowIndex   - the row index
	 * @param columnName - name of the column in this {@link DataFrame}.
	 * 
	 * @return the value for the specified row in the specified
	 *         {@code DateTimeColumn}.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 * @throws IllegalArgumentException  if {@code columnName} is not a recognized
	 *                                   column name in this dataframe.
	 * @throws ClassCastException        if the column is not a
	 *                                   {@code DateTimeColumn}
	 */
	LocalDateTime getDateTime(int rowIndex, String columnName);
}
