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

import tech.bitey.dataframe.ByteColumnBuilder;
import tech.bitey.dataframe.ColumnType;

/**
 * Logic for reading a {@code byte} value from a {@link ResultSet} into a
 * {@link ByteColumnBuilder}:
 * <ul>
 * <li>{@link #BYTE_FROM_BYTE}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum ByteFromResultSet implements IFromResultSet<Byte, ByteColumnBuilder> {
	/**
	 * Reads an {@code byte} from the {@code ResultSet} using
	 * {@link ResultSet#getByte(int)}, and adds it to the builder using
	 * {@link ByteColumnBuilder#add(byte)}
	 */
	BYTE_FROM_BYTE {
		@Override
		public void get(ResultSet rs, int columnIndex, ByteColumnBuilder builder) throws SQLException {

			final byte l = rs.getByte(columnIndex);

			if (rs.wasNull())
				builder.addNull();
			else
				builder.add(l);
		}
	};

	@Override
	public ColumnType<Byte> getColumnType() {
		return ColumnType.BYTE;
	}
}
