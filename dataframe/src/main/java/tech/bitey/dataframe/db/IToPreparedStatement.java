package tech.bitey.dataframe.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import tech.bitey.dataframe.Column;

public interface IToPreparedStatement<C extends Column<?>> {

	void set(C column, int rowIndex, PreparedStatement ps, int paramIndex) throws SQLException;
}
