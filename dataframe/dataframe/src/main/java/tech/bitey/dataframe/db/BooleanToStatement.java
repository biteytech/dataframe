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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import tech.bitey.dataframe.BooleanColumn;

public enum BooleanToStatement implements IToPreparedStatement<BooleanColumn> {
	BOOLEAN_TO_STRING {

		@Override
		public void set(BooleanColumn column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException {

			if (column.isNull(rowIndex))
				ps.setString(paramIndex, null);
			else {
				boolean value = column.getBoolean(rowIndex);

				ps.setString(paramIndex, value ? "Y" : "N");
			}
		}

	},
	BOOLEAN_TO_INT {

		@Override
		public void set(BooleanColumn column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException {

			if (column.isNull(rowIndex))
				ps.setNull(paramIndex, java.sql.Types.TINYINT);
			else {
				boolean value = column.getBoolean(rowIndex);

				ps.setInt(paramIndex, value ? 1 : 0);
			}
		}

	};
}
