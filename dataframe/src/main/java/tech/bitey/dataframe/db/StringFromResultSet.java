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

package tech.bitey.dataframe.db;

import java.sql.ResultSet;
import java.sql.SQLException;

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.StringColumnBuilder;

/**
 * Logic for reading a string from a {@link ResultSet} into a
 * {@link StringColumnBuilder}:
 * <ul>
 * <li>{@link #STRING_FROM_STRING}
 * <li>{@link #STRING_FROM_NSTRING}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum StringFromResultSet implements IFromResultSet<String, StringColumnBuilder> {
	/**
	 * Reads a string from the {@code ResultSet} using
	 * {@link ResultSet#getString(int)}, and adds it to the builder using
	 * {@code ColumnBuilder.add(String)}
	 */
	STRING_FROM_STRING {
		@Override
		public void get(ResultSet rs, int columnIndex, StringColumnBuilder builder) throws SQLException {

			final String s = rs.getString(columnIndex);

			builder.add(s);
		}
	},
	/**
	 * Reads a string from the {@code ResultSet} using
	 * {@link ResultSet#getNString(int)}, and adds it to the builder using
	 * {@code ColumnBuilder.add(String)}
	 */
	STRING_FROM_NSTRING {
		@Override
		public void get(ResultSet rs, int columnIndex, StringColumnBuilder builder) throws SQLException {

			final String s = rs.getNString(columnIndex);

			builder.add(s);
		}
	};

	@Override
	public ColumnType<String> getColumnType() {
		return ColumnType.STRING;
	}
}
