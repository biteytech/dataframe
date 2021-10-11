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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.InstantColumnBuilder;

/**
 * Logic for reading an {@link Instant} value from a {@link ResultSet} into a
 * {@link InstantColumnBuilder}:
 * <ul>
 * <li>{@link #INSTANT_FROM_TIMESTAMP}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum InstantFromResultSet implements IFromResultSet<Instant, InstantColumnBuilder> {
	INSTANT_FROM_TIMESTAMP {
		@Override
		public void get(ResultSet rs, int columnIndex, InstantColumnBuilder builder) throws SQLException {

			final Timestamp ts = rs.getTimestamp(columnIndex);

			if (ts == null)
				builder.addNull();
			else
				builder.add(ts.toInstant());
		}
	};

	@Override
	public ColumnType<Instant> getColumnType() {
		return ColumnType.INSTANT;
	}
}
