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

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.LongColumnBuilder;

/**
 * Logic for reading a {@code long} value from a {@link ResultSet} into a
 * {@link LongColumnBuilder}:
 * <ul>
 * <li>{@link #LONG_FROM_LONG}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum LongFromResultSet implements IFromResultSet<Long, LongColumnBuilder> {
	/**
	 * Reads an {@code long} from the {@code ResultSet} using
	 * {@link ResultSet#getLong(int)}, and adds it to the builder using
	 * {@link LongColumnBuilder#add(long)}
	 */
	LONG_FROM_LONG {
		@Override
		public void get(ResultSet rs, int columnIndex, LongColumnBuilder builder) throws SQLException {

			final long l = rs.getLong(columnIndex);

			if (rs.wasNull())
				builder.addNull();
			else
				builder.add(l);
		}
	};

	@Override
	public ColumnType<Long> getColumnType() {
		return ColumnType.LONG;
	}
}
