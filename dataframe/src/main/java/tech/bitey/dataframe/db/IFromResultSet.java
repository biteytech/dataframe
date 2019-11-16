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

package tech.bitey.dataframe.db;

import java.sql.ResultSet;
import java.sql.SQLException;

import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.ColumnBuilder;
import tech.bitey.dataframe.ColumnType;

/**
 * Logic for converting a column in a {@link ResultSet} into a {@link Column}
 * via a {@link ColumnBuilder}.
 * <p>
 * A number of pre-canned implementations have been provided:
 * <ul>
 * <li>{@link BooleanFromResultSet}
 * <li>{@link DateFromResultSet}
 * <li>{@link DateTimeFromResultSet}
 * <li>{@link DecimalFromResultSet}
 * <li>{@link DoubleFromResultSet}
 * <li>{@link FloatFromResultSet}
 * <li>{@link IntFromResultSet}
 * <li>{@link LongFromResultSet}
 * <li>{@link StringFromResultSet}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 *
 * @param <E> - the column's element type
 * @param <B> - the {@link ColumnBuilder} type
 */
public interface IFromResultSet<E, B extends ColumnBuilder<E>> {

	/**
	 * Returns the {@link ColumnType} used to create the {@link ColumnBuilder} (via
	 * {@link ColumnType#builder()}).
	 * 
	 * @return the {@code ColumnType} used to create the {@code ColumnBuilder}
	 */
	ColumnType<E> getColumnType();

	/**
	 * Gets one elements from the {@link ResultSet} at the current row, for the
	 * specified column, and adds it to the builder.
	 * 
	 * @param rs          - the {@link ResultSet}
	 * @param columnIndex - one-based index of a column in the {@code ResultSet}
	 * @param builder     - the {@link ColumnBuilder} into which elements are added
	 * 
	 * @throws SQLException if some SQL or database error occurs
	 */
	void get(ResultSet rs, int columnIndex, B builder) throws SQLException;
}
