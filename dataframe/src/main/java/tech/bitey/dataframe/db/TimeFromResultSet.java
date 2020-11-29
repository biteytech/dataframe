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
import java.time.LocalTime;

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.TimeColumnBuilder;

/**
 * Logic for reading a {@link LocalTime} value from a {@link ResultSet} into a
 * {@link TimeColumnBuilder}:
 * <ul>
 * <li>{@link #TIME_FROM_STRING}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum TimeFromResultSet implements IFromResultSet<LocalTime, TimeColumnBuilder> {
	/**
	 * Reads an {@link LocalTime} from the {@code ResultSet} using
	 * {@link ResultSet#getString(int)}.
	 */
	TIME_FROM_STRING {
		@Override
		public void get(ResultSet rs, int columnIndex, TimeColumnBuilder builder) throws SQLException {

			final String text = rs.getString(columnIndex);

			if (text == null)
				builder.addNull();
			else
				builder.add(LocalTime.parse(text));
		}
	},;

	@Override
	public ColumnType<LocalTime> getColumnType() {
		return ColumnType.TIME;
	}
}
