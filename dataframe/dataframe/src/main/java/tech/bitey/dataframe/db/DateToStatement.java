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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import tech.bitey.dataframe.DateColumn;

public enum DateToStatement implements IToPreparedStatement<DateColumn> {
	DATE_TO_DATE {

		@Override
		public void set(DateColumn column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException {

			if (column.isNull(rowIndex))
				ps.setDate(paramIndex, null);
			else {
				int yyyymmdd = column.yyyymmdd(rowIndex);

				@SuppressWarnings("deprecation")
				Date date = new Date(yyyymmdd / 10000 - 1900, yyyymmdd % 10000 / 100 - 1, yyyymmdd % 100);
				ps.setDate(paramIndex, date);
			}
		}

	},
	DATE_TO_INT {

		@Override
		public void set(DateColumn column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException {

			if (column.isNull(rowIndex))
				ps.setNull(paramIndex, java.sql.Types.INTEGER);
			else {
				int yyyymmdd = column.yyyymmdd(rowIndex);

				ps.setInt(paramIndex, yyyymmdd);
			}
		}

	};
}
