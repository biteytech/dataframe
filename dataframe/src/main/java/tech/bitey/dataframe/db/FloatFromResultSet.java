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

package tech.bitey.dataframe.db;

import java.sql.ResultSet;
import java.sql.SQLException;

import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.FloatColumnBuilder;

/**
 * Logic for reading a {@code float} value from a {@link ResultSet} into a
 * {@link FloatColumnBuilder}:
 * <ul>
 * <li>{@link #FLOAT_FROM_FLOAT}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum FloatFromResultSet implements IFromResultSet<Float, FloatColumnBuilder> {
	/**
	 * Reads an {@code float} from the {@code ResultSet} using
	 * {@link ResultSet#getFloat(int)}, and adds it to the builder using
	 * {@link FloatColumnBuilder#add(float)}
	 */
	FLOAT_FROM_FLOAT {
		@Override
		public void get(ResultSet rs, int columnIndex, FloatColumnBuilder builder) throws SQLException {

			final float f = rs.getFloat(columnIndex);

			if (rs.wasNull())
				builder.addNull();
			else
				builder.add(f);
		}
	};

	@Override
	public ColumnType<Float> getColumnType() {
		return ColumnType.FLOAT;
	}
}
