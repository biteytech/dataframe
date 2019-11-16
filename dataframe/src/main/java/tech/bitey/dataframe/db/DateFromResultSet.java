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

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import tech.bitey.dataframe.ColumnBuilder;
import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.DateColumnBuilder;

/**
 * Logic for reading a date value from a {@link ResultSet} into a
 * {@link DateColumnBuilder}:
 * <p>
 * <ul>
 * <li>{@link #DATE_FROM_DATE}
 * <li>{@link #DATE_FROM_INT}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum DateFromResultSet implements IFromResultSet<DateColumnBuilder> {
	/**
	 * Reads an {@link Date} from the {@code ResultSet} using
	 * {@link ResultSet#getDate(int)}, and adds it to the builder using
	 * {@link ColumnBuilder#add(int, int, int)}
	 */
	@SuppressWarnings("deprecation")
	DATE_FROM_DATE {
		@Override
		public void get(ResultSet rs, int columnIndex, DateColumnBuilder builder) throws SQLException {

			final Date date = rs.getDate(columnIndex);

			if (date == null)
				builder.addNull();
			else
				builder.add(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
		}
	},
	/**
	 * Reads a {@code yyyymmdd int} value from the {@code ResultSet} using
	 * {@link ResultSet#getInt(int)}. The {@code yyyymmdd} value is added to the
	 * builder using {@link DateColumnBuilder#add(int, int, int)
	 */
	DATE_FROM_INT {
		@Override
		public void get(ResultSet rs, int columnIndex, DateColumnBuilder builder) throws SQLException {

			final int yyyymmdd = rs.getInt(columnIndex);

			if (rs.wasNull())
				builder.addNull();
			else
				builder.add(yyyymmdd / 10000, yyyymmdd / 100 % 100, yyyymmdd % 100);
		}
	};

	@Override
	public ColumnType getColumnType() {
		return ColumnType.DATE;
	}
}
