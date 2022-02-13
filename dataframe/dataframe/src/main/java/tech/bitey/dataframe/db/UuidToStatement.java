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

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import tech.bitey.dataframe.UuidColumn;

public enum UuidToStatement implements IToPreparedStatement<UuidColumn> {
	UUID_TO_STRING {

		@Override
		public void set(UuidColumn column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException {

			if (column.isNull(rowIndex))
				ps.setString(paramIndex, null);
			else {
				ps.setString(paramIndex, column.get(rowIndex).toString());
			}
		}

	},
	UUID_TO_UPPER_STRING {

		@Override
		public void set(UuidColumn column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException {

			if (column.isNull(rowIndex))
				ps.setString(paramIndex, null);
			else {
				ps.setString(paramIndex, column.get(rowIndex).toString().toUpperCase());
			}
		}

	},
	UUID_TO_BYTES {

		@Override
		public void set(UuidColumn column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException {

			if (column.isNull(rowIndex))
				ps.setBytes(paramIndex, null);
			else {
				UUID uuid = column.get(rowIndex);
				ByteBuffer bb = ByteBuffer.allocate(16);
				bb.putLong(uuid.getMostSignificantBits());
				bb.putLong(uuid.getLeastSignificantBits());
				ps.setBytes(paramIndex, bb.array());
			}
		}

	};
}
