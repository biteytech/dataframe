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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Objects;

class DataFrameResultSetMetaData implements ResultSetMetaData {

	private final DataFrame df;

	DataFrameResultSetMetaData(DataFrame df) {
		Objects.requireNonNull(df, "dataframe cannot be null");
		this.df = df;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return df.columnCount();
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return df.column(column - 1).isNonnull() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		switch (df.column(column - 1).getType().getCode()) {
		case D:
		case F:
		case I:
		case L:
		case T:
		case Y:
		case BD:
			return true;
		default:
			return false;
		}
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return getColumnName(column);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return df.columnName(column - 1);
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getColumnClassName(column);
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return df.column(column - 1).getType().getElementType().getName();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("not a wrapper");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return "";
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return "";
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return "";
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return java.sql.Types.JAVA_OBJECT;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}
}
