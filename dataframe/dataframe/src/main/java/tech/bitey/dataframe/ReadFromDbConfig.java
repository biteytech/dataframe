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

package tech.bitey.dataframe;

import static tech.bitey.dataframe.Pr.checkArgument;
import static tech.bitey.dataframe.Pr.checkState;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import tech.bitey.dataframe.db.IFromResultSet;

/**
 * Configuration for reading a dataframe from a {@link ResultSet}. The two
 * configurable settings are:
 * <ul>
 * <li>Mandatory per-column logic for getting data from a {@code ResultSet}. See
 * {@link IFromResultSet} for details.
 * <li>Optional fetch size - passed to {@link ResultSet#setFetchSize(int)}.
 * Default {@code 1000}.
 * </ul>
 * 
 * @author biteytech@protonmail.com
 * 
 * @see DataFrameFactory#readFrom(ResultSet, ReadFromDbConfig)
 * @see IFromResultSet
 */
public record ReadFromDbConfig(List<IFromResultSet<?, ?>> fromRsLogic, int fetchSize) {

	public ReadFromDbConfig {
		checkArgument(fetchSize >= 1, "fetch size must be strictly positive");

		fromRsLogic = List.copyOf(fromRsLogic);
	}

	public ReadFromDbConfig(List<IFromResultSet<?, ?>> fromRsLogic) {
		this(fromRsLogic, 1000);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	DataFrame read(ResultSet rs) throws SQLException {

		ResultSetMetaData metaData = rs.getMetaData();

		final int cc = metaData.getColumnCount();

		checkState(cc == fromRsLogic.size(), "mismatched column count");

		ColumnBuilder[] builders = new ColumnBuilder[cc];
		String[] columnNames = new String[cc];
		IFromResultSet[] fromRsLogic = new IFromResultSet[cc];

		for (int i = 0; i < cc; i++) {
			columnNames[i] = metaData.getColumnName(i + 1);
			fromRsLogic[i] = this.fromRsLogic.get(i);
			builders[i] = fromRsLogic[i].getColumnType().builder();
		}

		rs.setFetchSize(fetchSize);

		while (rs.next())
			for (int i = 0; i < cc; i++)
				fromRsLogic[i].get(rs, i + 1, builders[i]);

		Column<?>[] columns = new Column<?>[cc];
		for (int i = 0; i < columns.length; i++)
			columns[i] = builders[i].build();

		return DataFrameFactory.create(columns, columnNames);
	}
}
