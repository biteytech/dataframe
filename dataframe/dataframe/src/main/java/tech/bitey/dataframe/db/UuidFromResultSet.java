/*
 * Copyright 2021 biteytech@protonmail.com
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

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.UuidColumnBuilder;

/**
 * Logic for reading a {@link UUID} value from a {@link ResultSet} into a
 * {@link UuidColumnBuilder}:
 * <ul>
 * <li>{@link #UUID_FROM_STRING}
 * <li>{@link #UUID_FROM_BYTES}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum UuidFromResultSet implements IFromResultSet<UUID, UuidColumnBuilder> {
	/**
	 * Reads an {@link UUID} from the {@code ResultSet} using
	 * {@link ResultSet#getString(int)}.
	 */
	UUID_FROM_STRING {
		@Override
		public void get(ResultSet rs, int columnIndex, UuidColumnBuilder builder) throws SQLException {

			final String text = rs.getString(columnIndex);

			if (text == null)
				builder.addNull();
			else
				builder.add(UUID.fromString(text));
		}
	},
	/**
	 * Reads a {@link UUID} value from the {@code ResultSet} using
	 * {@link ResultSet#getBytes(int)}.
	 */
	UUID_FROM_BYTES {
		@Override
		public void get(ResultSet rs, int columnIndex, UuidColumnBuilder builder) throws SQLException {

			final byte[] bytes = rs.getBytes(columnIndex);

			if (bytes == null)
				builder.addNull();
			else {
				ByteBuffer bb = ByteBuffer.wrap(bytes);
				builder.add(bb.getLong(), bb.getLong());
			}
		}
	},;

	@Override
	public ColumnType<UUID> getColumnType() {
		return ColumnType.UUID;
	}
}
