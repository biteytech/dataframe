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

import tech.bitey.dataframe.db.BlobToStatement;
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
				toPsLogic[i] = switch (columns[i].getType().getCode()) {
				case B -> BooleanToStatement.BOOLEAN_TO_STRING;
				case DA -> DateToStatement.DATE_TO_DATE;
				case DT -> DateTimeToStatement.DATETIME_TO_TIMESTAMP;
				case TI -> TimeToStatement.TIME_TO_STRING;
				case IN -> InstantToStatement.INSTANT_TO_TIMESTAMP;
				case BD -> DecimalToStatement.BIGDECIMAL_TO_BIGDECIMAL;
				case D -> DoubleToStatement.DOUBLE_TO_DOUBLE;
				case F -> FloatToStatement.FLOAT_TO_FLOAT;
				case I -> IntToStatement.INT_TO_INT;
				case L -> LongToStatement.LONG_TO_LONG;
				case T -> ShortToStatement.SHORT_TO_SHORT;
				case Y -> ByteToStatement.BYTE_TO_BYTE;
				case S -> StringToStatement.STRING_TO_STRING;
				case UU -> UuidToStatement.UUID_TO_STRING;
				case NS -> NormalStringToStatement.STRING_TO_STRING;
				case BL -> BlobToStatement.INPUT_STREAM;
				};
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
