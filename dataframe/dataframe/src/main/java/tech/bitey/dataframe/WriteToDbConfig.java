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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import tech.bitey.dataframe.db.BooleanToStatement;
import tech.bitey.dataframe.db.ByteToStatement;
import tech.bitey.dataframe.db.DateTimeToStatement;
import tech.bitey.dataframe.db.DateToStatement;
import tech.bitey.dataframe.db.DecimalToStatement;
import tech.bitey.dataframe.db.DoubleToStatement;
import tech.bitey.dataframe.db.FloatToStatement;
import tech.bitey.dataframe.db.IFromResultSet;
import tech.bitey.dataframe.db.IToPreparedStatement;
import tech.bitey.dataframe.db.InstantToStatement;
import tech.bitey.dataframe.db.IntToStatement;
import tech.bitey.dataframe.db.LongToStatement;
import tech.bitey.dataframe.db.NormalStringToStatement;
import tech.bitey.dataframe.db.ShortToStatement;
import tech.bitey.dataframe.db.StringToStatement;
import tech.bitey.dataframe.db.TimeToStatement;
import tech.bitey.dataframe.db.UuidToStatement;

/**
 * Configuration for writing a dataframe to a database via a
 * {@link PreparedStatement}. The three configurable settings are:
 * <ul>
 * <li>Per-column logic for setting data to the {@code PreparedStatement}. See
 * {@link IToPreparedStatement} for details.
 * <li>Optional batch size. Defaults to {@code 1000}.
 * <li>Whether or not to call commit. Defaults to {@code true}.
 * </ul>
 * 
 * @author biteytech@protonmail.com
 * 
 * @see DataFrameFactory#readFrom(ResultSet, ReadFromDbConfig)
 * @see IFromResultSet
 */
public record WriteToDbConfig(List<IToPreparedStatement<?>> toPsLogic, int batchSize, boolean commit) {

	private static final int DEFAULT_BATCH_SIZE = 1000;

	public static final WriteToDbConfig DEFAULT_CONFIG = new WriteToDbConfig(null);

	public WriteToDbConfig {
		checkArgument(batchSize >= 1, "batch size must be strictly positive");

		toPsLogic = toPsLogic == null ? null : List.copyOf(toPsLogic);
	}

	public WriteToDbConfig(List<IToPreparedStatement<?>> toPsLogic) {
		this(toPsLogic, DEFAULT_BATCH_SIZE, true);
	}

	public WriteToDbConfig(List<IToPreparedStatement<?>> toPsLogic, int batchSize) {
		this(toPsLogic, batchSize, true);
	}

	public WriteToDbConfig(List<IToPreparedStatement<?>> toPsLogic, boolean commit) {
		this(toPsLogic, DEFAULT_BATCH_SIZE, commit);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	void write(DataFrame dataframe, PreparedStatement ps) throws SQLException {

		final Connection conn = ps.getConnection();
		checkState(!conn.getAutoCommit(), "autocommit must be off");

		Column[] columns = dataframe.columns().toArray(new Column[0]);

		IToPreparedStatement[] toPsLogic = this.toPsLogic == null ? null
				: this.toPsLogic.toArray(new IToPreparedStatement[0]);

		if (toPsLogic == null) {
			toPsLogic = new IToPreparedStatement[columns.length];

			for (int i = 0; i < columns.length; i++) {
				switch (columns[i].getType().getCode()) {
				case B:
					toPsLogic[i] = BooleanToStatement.BOOLEAN_TO_STRING;
					break;
				case DA:
					toPsLogic[i] = DateToStatement.DATE_TO_DATE;
					break;
				case DT:
					toPsLogic[i] = DateTimeToStatement.DATETIME_TO_TIMESTAMP;
					break;
				case TI:
					toPsLogic[i] = TimeToStatement.TIME_TO_STRING;
					break;
				case IN:
					toPsLogic[i] = InstantToStatement.INSTANT_TO_TIMESTAMP;
					break;
				case BD:
					toPsLogic[i] = DecimalToStatement.BIGDECIMAL_TO_BIGDECIMAL;
					break;
				case D:
					toPsLogic[i] = DoubleToStatement.DOUBLE_TO_DOUBLE;
					break;
				case F:
					toPsLogic[i] = FloatToStatement.FLOAT_TO_FLOAT;
					break;
				case I:
					toPsLogic[i] = IntToStatement.INT_TO_INT;
					break;
				case L:
					toPsLogic[i] = LongToStatement.LONG_TO_LONG;
					break;
				case T:
					toPsLogic[i] = ShortToStatement.SHORT_TO_SHORT;
					break;
				case Y:
					toPsLogic[i] = ByteToStatement.BYTE_TO_BYTE;
					break;
				case S:
					toPsLogic[i] = StringToStatement.STRING_TO_STRING;
					break;
				case UU:
					toPsLogic[i] = UuidToStatement.UUID_TO_STRING;
					break;
				case NS:
					toPsLogic[i] = NormalStringToStatement.STRING_TO_STRING;
					break;
				}
			}
		}

		for (int r = 0; r < dataframe.size();) {
			for (int c = 0; c < columns.length; c++)
				toPsLogic[c].set(columns[c], r, ps, c + 1);

			ps.addBatch();

			if (++r % batchSize == 0)
				ps.executeBatch();
		}

		if (dataframe.size() % batchSize != 0)
			ps.executeBatch();

		if (commit)
			conn.commit();
	}
}
