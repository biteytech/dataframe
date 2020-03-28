/*
 * Copyright 2020 biteytech@protonmail.com
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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.DateColumnBuilder;
import tech.bitey.dataframe.DecimalColumnBuilder;

/**
 * Logic for reading a {@link BigDecimal} value from a {@link ResultSet} into a
 * {@link DateColumnBuilder}:
 * <ul>
 * <li>{@link #BIGDECIMAL_FROM_BIGDECIMAL}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum DecimalFromResultSet implements IFromResultSet<BigDecimal, DecimalColumnBuilder> {
	/**
	 * Reads a {@link BigDecimal} from the {@code ResultSet} using
	 * {@link ResultSet#getBigDecimal(int)}, and adds it to the builder using
	 * {@code ColumnBuilder.add(BigDecimal)}
	 */
	BIGDECIMAL_FROM_BIGDECIMAL {
		@Override
		public void get(ResultSet rs, int columnIndex, DecimalColumnBuilder builder) throws SQLException {

			final BigDecimal bd = rs.getBigDecimal(columnIndex);

			builder.add(bd);
		}
	};

	@Override
	public ColumnType<BigDecimal> getColumnType() {
		return ColumnType.DECIMAL;
	}
}
