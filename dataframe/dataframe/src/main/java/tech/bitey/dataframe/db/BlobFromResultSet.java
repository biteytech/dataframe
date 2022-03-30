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

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import tech.bitey.dataframe.BlobColumnBuilder;
import tech.bitey.dataframe.ColumnType;

/**
 * Logic for reading an {@link InputStream} value from a {@link ResultSet} into
 * a {@link BlobColumnBuilder}:
 * <ul>
 * <li>{@link #INPUT_STREAM}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum BlobFromResultSet implements IFromResultSet<InputStream, BlobColumnBuilder> {
	/**
	 * Reads an {@link InputStream} from the {@code ResultSet} using
	 * {@link ResultSet#getBinaryStream(int)}, and adds it to the builder using
	 * {@code ColumnBuilder.add(InputStream)}
	 */
	INPUT_STREAM {
		@Override
		public void get(ResultSet rs, int columnIndex, BlobColumnBuilder builder) throws SQLException {

			final InputStream is = rs.getBinaryStream(columnIndex);

			builder.add(is);
		}
	};

	@Override
	public ColumnType<InputStream> getColumnType() {
		return ColumnType.BLOB;
	}
}
