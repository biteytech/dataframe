/*
 * Copyright 2022 biteytech@protonmail.com
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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.DateTimeColumnBuilder;

/**
 * Logic for reading a date-time value from a {@link ResultSet} into a
 * {@link DateTimeColumnBuilder}:
 * <ul>
 * <li>{@link #DATETIME_FROM_TIMESTAMP}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum DateTimeFromResultSet implements IFromResultSet<LocalDateTime, DateTimeColumnBuilder> {
	/**
	 * Reads an {@link Timestamp} from the {@code ResultSet} using
	 * {@link ResultSet#getTimestamp(int)}, and adds it to the builder using
	 * {@link Timestamp#toLocalDateTime()} and
	 * {@code ColumnBuilder.add(java.time.LocalDateTime)}
	 */
	DATETIME_FROM_TIMESTAMP {
		@Override
		public void get(ResultSet rs, int columnIndex, DateTimeColumnBuilder builder) throws SQLException {

			final Timestamp ts = rs.getTimestamp(columnIndex);

			if (ts == null)
				builder.addNull();
			else
				builder.add(ts.toLocalDateTime());
		}
	};

	/**
	 * Reads a value from the {@code ResultSet} using
	 * {@link ResultSet#getString(int)} and parses it using the specified
	 * {@link DateTimeFormatter}.
	 */
	public static IFromResultSet<LocalDateTime, DateTimeColumnBuilder> of(DateTimeFormatter formatter)
			throws SQLException {

		return new IFromResultSet<LocalDateTime, DateTimeColumnBuilder>() {

			@Override
			public void get(ResultSet rs, int columnIndex, DateTimeColumnBuilder builder) throws SQLException {

				final String text = rs.getString(columnIndex);

				if (text == null)
					builder.addNull();
				else
					builder.add(LocalDateTime.parse(text, formatter));
			}

			@Override
			public ColumnType<LocalDateTime> getColumnType() {
				return ColumnType.DATETIME;
			}
		};
	};

	@Override
	public ColumnType<LocalDateTime> getColumnType() {
		return ColumnType.DATETIME;
	}
}
