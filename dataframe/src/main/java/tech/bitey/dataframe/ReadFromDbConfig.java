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

package tech.bitey.dataframe;

import static tech.bitey.dataframe.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.DfPreconditions.checkNotNull;
import static tech.bitey.dataframe.DfPreconditions.checkState;

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
 * <li>Optional fetch size - passed to {@link ResultSet#setFetchSize(int)}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 * 
 * @see DataFrameFactory#readFrom(ResultSet, ReadFromDbConfig)
 * @see IFromResultSet
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ReadFromDbConfig {

	private final IFromResultSet[] fromRsLogic;
	private final int fetchSize;

	/**
	 * Creates a configuration for reading a dataframe from a {@link ResultSet}.
	 * 
	 * @param fromResultSetLogic - Per-column logic for getting data from a
	 *                           {@code ResultSet}. See {@link IFromResultSet} for
	 *                           details. The size of this list must match the
	 *                           number of columns in the {@code ResultSet}.
	 * @param fetchSize          - passed to {@link ResultSet#setFetchSize(int)}
	 * 
	 * @throws NullPointerException     if {@code fromResultSetLogic} is null
	 * @throws IllegalArgumentException if {@code fromResultSetLogic} is empty or
	 *                                  {@code fetchSize}
	 */
	public ReadFromDbConfig(List<IFromResultSet<?, ?>> fromResultSetLogic, int fetchSize) {

		checkNotNull(fromResultSetLogic, "result set logic list cannot be null");
		checkArgument(!fromResultSetLogic.isEmpty(), "result set logic list cannot be empty");
		checkArgument(fetchSize >= 1, "fetch size must be strictly positive");

		this.fromRsLogic = fromResultSetLogic.toArray(new IFromResultSet[0]);
		this.fetchSize = fetchSize;
	}

	/**
	 * Creates a configuration for reading a dataframe from a {@link ResultSet},
	 * with a fetch size of 1000.
	 * 
	 * @param fromResultSetLogic - Per-column logic for getting data from a
	 *                           {@code ResultSet}. See {@link IFromResultSet} for
	 *                           details. The size of this list must match the
	 *                           number of columns in the {@code ResultSet}.
	 * 
	 * @throws NullPointerException     if {@code fromResultSetLogic} is null
	 * @throws IllegalArgumentException if {@code fromResultSetLogic} is empty or
	 *                                  {@code fetchSize}
	 */
	public ReadFromDbConfig(List<IFromResultSet<?, ?>> fromResultSetLogic) {
		this(fromResultSetLogic, 1000);
	}

	DataFrame read(ResultSet rs) throws SQLException {

		ResultSetMetaData metaData = rs.getMetaData();

		final int cc = metaData.getColumnCount();

		checkState(cc == fromRsLogic.length, "mismatched column count");

		ColumnBuilder[] builders = new ColumnBuilder[cc];
		String[] columnNames = new String[cc];

		for (int i = 0; i < cc; i++) {
			columnNames[i] = metaData.getColumnName(i + 1);
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
