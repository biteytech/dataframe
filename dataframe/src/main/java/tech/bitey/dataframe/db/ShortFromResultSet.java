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
import tech.bitey.dataframe.ShortColumnBuilder;

/**
 * Logic for reading a {@code short} value from a {@link ResultSet} into a
 * {@link ShortColumnBuilder}:
 * <ul>
 * <li>{@link #SHORT_FROM_SHORT}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum ShortFromResultSet implements IFromResultSet<Short, ShortColumnBuilder> {
	/**
	 * Reads an {@code short} from the {@code ResultSet} using
	 * {@link ResultSet#getShort(int)}, and adds it to the builder using
	 * {@link ShortColumnBuilder#add(short)}
	 */
	SHORT_FROM_SHORT {
		@Override
		public void get(ResultSet rs, int columnIndex, ShortColumnBuilder builder) throws SQLException {

			final short l = rs.getShort(columnIndex);

			if (rs.wasNull())
				builder.addNull();
			else
				builder.add(l);
		}
	};

	@Override
	public ColumnType<Short> getColumnType() {
		return ColumnType.SHORT;
	}
}
