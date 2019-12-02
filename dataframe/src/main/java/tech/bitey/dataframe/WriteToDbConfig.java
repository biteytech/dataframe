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

import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import tech.bitey.dataframe.db.BooleanToStatement;
import tech.bitey.dataframe.db.ByteToStatement;
import tech.bitey.dataframe.db.DateTimeToStatement;
import tech.bitey.dataframe.db.DateToStatement;
import tech.bitey.dataframe.db.DecimalToStatement;
import tech.bitey.dataframe.db.DoubleToStatement;
import tech.bitey.dataframe.db.FloatToStatement;
import tech.bitey.dataframe.db.IToPreparedStatement;
import tech.bitey.dataframe.db.IntToStatement;
import tech.bitey.dataframe.db.LongToStatement;
import tech.bitey.dataframe.db.ShortToStatement;
import tech.bitey.dataframe.db.StringToStatement;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class WriteToDbConfig {

	public static final WriteToDbConfig DEFAULT_CONFIG = new WriteToDbConfig(null);

	private final IToPreparedStatement[] toPsLogic;
	private final int batchSize;
	private final boolean commit;

	public WriteToDbConfig(List<IToPreparedStatement<?>> toPsLogic, int batchSize, boolean commit) {

		this.toPsLogic = toPsLogic == null ? null : toPsLogic.toArray(new IToPreparedStatement[0]);
		this.batchSize = batchSize;
		this.commit = commit;
	}

	public WriteToDbConfig(List<IToPreparedStatement<?>> toPsLogic, boolean commit) {
		this(toPsLogic, 1000, commit);
	}

	public WriteToDbConfig(List<IToPreparedStatement<?>> toPsLogic) {
		this(toPsLogic, 1000, true);
	}

	void write(DataFrame dataframe, PreparedStatement ps) throws SQLException {

		final Connection conn = ps.getConnection();
		checkState(!conn.getAutoCommit(), "autocommit must be off");

		Column[] columns = dataframe.columns().toArray(new Column[0]);

		IToPreparedStatement[] toPsLogic = this.toPsLogic;

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
					toPsLogic[i] = DateTimeToStatement.DATETIME_FROM_TIMESTAMP;
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
				}
			}
		}

		for (int r = 0; r < dataframe.size();) {
			for (int c = 0; c < columns.length; c++)
				toPsLogic[c].set(columns[c], r, ps, c + 1);

			ps.addBatch();

			if (++r % batchSize == 0) {
				ps.executeBatch();
				if (commit)
					conn.commit();
			}
		}

		if (dataframe.size() % batchSize != 0) {
			ps.executeBatch();
			if (commit)
				conn.commit();
		}
	}
}
