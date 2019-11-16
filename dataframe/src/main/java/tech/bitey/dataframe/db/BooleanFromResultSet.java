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

import tech.bitey.dataframe.BooleanColumnBuilder;
import tech.bitey.dataframe.ColumnType;

/**
 * Logic for reading a boolean value from a {@link ResultSet} into a
 * {@link BooleanColumnBuilder}:
 * <ul>
 * <li>{@link #BOOLEAN_FROM_STRING}
 * <li>{@link #BOOLEAN_FROM_INT}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum BooleanFromResultSet implements IFromResultSet<Boolean, BooleanColumnBuilder> {
	/**
	 * Reads a string value from the {@code ResultSet} using
	 * {@link ResultSet#getString(int)}. The string value is considered to be
	 * {@code true} iff it is "Y" or "true", ignoring case.
	 */
	BOOLEAN_FROM_STRING {
		@Override
		public void get(ResultSet rs, int columnIndex, BooleanColumnBuilder builder) throws SQLException {

			final String s = rs.getString(columnIndex);

			if (s == null)
				builder.addNull();
			else
				builder.add("Y".equalsIgnoreCase(s) || "true".equalsIgnoreCase(s));
		}
	},
	/**
	 * Reads an {@code int} value from the {@code ResultSet} using
	 * {@link ResultSet#getInt(int)}. The {@code int} value is considered to be
	 * {@code true} iff it is not zero.
	 */
	BOOLEAN_FROM_INT {
		@Override
		public void get(ResultSet rs, int columnIndex, BooleanColumnBuilder builder) throws SQLException {

			final int i = rs.getInt(columnIndex);

			if (rs.wasNull())
				builder.addNull();
			else
				builder.add(i != 0);
		}
	};

	/**
	 * Returns {@link ColumnType#BOOLEAN}
	 * 
	 * @return {@code ColumnType.BOOLEAN}
	 */
	@Override
	public ColumnType<Boolean> getColumnType() {
		return ColumnType.BOOLEAN;
	}
}
