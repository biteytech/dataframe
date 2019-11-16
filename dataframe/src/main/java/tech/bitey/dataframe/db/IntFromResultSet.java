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
import tech.bitey.dataframe.IntColumnBuilder;

/**
 * Logic for reading a {@code int} value from a {@link ResultSet} into a
 * {@link IntColumnBuilder}:
 * <p>
 * <ul>
 * <li>{@link #INT_FROM_INT}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum IntFromResultSet implements IFromResultSet<IntColumnBuilder> {
	/**
	 * Reads an {@code int} from the {@code ResultSet} using
	 * {@link ResultSet#getInt(int)}, and adds it to the builder using
	 * {@link IntColumnBuilder#add(int)}
	 */
	INT_FROM_INT {
		@Override
		public void get(ResultSet rs, int columnIndex, IntColumnBuilder builder) throws SQLException {

			final int i = rs.getInt(columnIndex);

			if (rs.wasNull())
				builder.addNull();
			else
				builder.add(i);
		}
	};

	@Override
	public ColumnType getColumnType() {
		return ColumnType.INT;
	}
}
