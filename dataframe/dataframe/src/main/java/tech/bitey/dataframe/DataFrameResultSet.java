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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Map;

/**
 * A {@link ResultSet} implementation backed by a {@link DataFrame}. Supports
 * bidirectional cursor navigation. Does not support {@code updateXXX} methods.
 *
 * @author biteytech@protonmail.com
 */
public class DataFrameResultSet implements ResultSet {

	private static final int BEFORE_FIRST = 0;

	private DataFrame df;

	private final int AFTER_LAST;
	private int row = BEFORE_FIRST;

	private boolean wasNull = false;

	DataFrameResultSet(DataFrame df) {
		this.df = df;
		this.AFTER_LAST = df.size() + 1;
	}

	private static void throwReadOnly() throws SQLException {
		throw new SQLException("result set is read-only (ResultSet.CONCUR_READ_ONLY)");
	}

	private void checkClosed() throws SQLException {
		if (isClosed())
			throw new SQLException("result set is closed");
	}

	private int rowIndex() throws SQLException {
		checkClosed();
		if (row == BEFORE_FIRST)
			throw new SQLException("before first row");
		else if (row == AFTER_LAST)
			throw new SQLException("after last row");
		else
			return row - 1;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface == DataFrame.class;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		checkClosed();

		if (isWrapperFor(iface))
			return (T) df;
		else
			throw new SQLException("could not unwrap type: " + iface);
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		checkClosed();

		if (row < BEFORE_FIRST) {
			row += AFTER_LAST;
			if (row < BEFORE_FIRST)
				row = BEFORE_FIRST;
		}
		if (row > AFTER_LAST)
			row = AFTER_LAST;

		this.row = row;
		return row != BEFORE_FIRST && row != AFTER_LAST;
	}

	@Override
	public void afterLast() throws SQLException {
		checkClosed();
		row = AFTER_LAST;
	}

	@Override
	public void beforeFirst() throws SQLException {
		checkClosed();
		row = BEFORE_FIRST;
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throwReadOnly();
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public void close() throws SQLException {
		df = null;
	}

	@Override
	public void deleteRow() throws SQLException {
		throwReadOnly();
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		checkClosed();
		try {
			return df.columnIndex(columnLabel) + 1;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean first() throws SQLException {
		return absolute(1);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getArray");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getArray");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getAsciiStream");
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getAsciiStream");
	}

	private BigDecimal getBigDecimal0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return null;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case BD -> df.getBigDecimal(rowIndex, columnIndex);
		case S, NS, FS -> new BigDecimal(df.getString(rowIndex, columnIndex));
		case D -> BigDecimal.valueOf(df.getDouble(rowIndex, columnIndex));
		case F -> BigDecimal.valueOf(df.getFloat(rowIndex, columnIndex));
		case L -> BigDecimal.valueOf(df.getLong(rowIndex, columnIndex));
		case I -> BigDecimal.valueOf(df.getInt(rowIndex, columnIndex));
		case T -> BigDecimal.valueOf(df.getShort(rowIndex, columnIndex));
		case Y -> BigDecimal.valueOf(df.getByte(rowIndex, columnIndex));
		default -> throw new SQLException("cannot convert from " + type + " to BigDecimal");
		};
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getBigDecimal0(columnIndex - 1);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal0(df.columnIndex(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("BigDecimal scale");
	}

	@Override
	public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("BigDecimal scale");
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBinaryStream");
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBinaryStream");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBlob");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBlob");
	}

	private boolean getBoolean0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return false;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		switch (type) {
		case B: {
			return df.getBoolean(rowIndex, columnIndex);
		}
		case BD: {
			BigDecimal bd = df.getBigDecimal(rowIndex, columnIndex);
			if (BigDecimal.ZERO.equals(bd))
				return false;
			else if (BigDecimal.ONE.equals(bd))
				return true;
			break;
		}
		case S:
		case NS:
		case FS: {
			String s = df.getString(rowIndex, columnIndex);
			if (s.length() > 5)
				break;
			switch (s.toUpperCase()) {
			case "0":
			case "N":
			case "FALSE":
				return false;
			case "1":
			case "Y":
			case "TRUE":
				return true;
			}
			break;
		}
		case D: {
			double d = df.getDouble(rowIndex, columnIndex);
			if (d == 0d)
				return false;
			else if (d == 1d)
				return true;
			break;
		}
		case F: {
			float f = df.getFloat(rowIndex, columnIndex);
			if (f == 0f)
				return false;
			else if (f == 1f)
				return true;
			break;
		}
		case L: {
			long l = df.getLong(rowIndex, columnIndex);
			if (l == 0l)
				return false;
			else if (l == 1l)
				return true;
			break;
		}
		case I: {
			int i = df.getInt(rowIndex, columnIndex);
			if (i == 0)
				return false;
			else if (i == 1)
				return true;
			break;
		}
		case T: {
			short t = df.getShort(rowIndex, columnIndex);
			if (t == 0)
				return false;
			else if (t == 1)
				return true;
			break;
		}
		case Y: {
			byte y = df.getByte(rowIndex, columnIndex);
			if (y == 0)
				return false;
			else if (y == 1)
				return true;
			break;
		}
		default:
			throw new SQLException("cannot convert from " + type + " to boolean");
		}

		throw new SQLException("cannot convert to boolean");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return getBoolean0(columnIndex - 1);
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean0(df.columnIndex(columnLabel));
	}

	private byte getByte0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return 0;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case BD -> df.getBigDecimal(rowIndex, columnIndex).byteValue();
		case S, NS, FS -> Byte.parseByte(df.getString(rowIndex, columnIndex));
		case D -> (byte) df.getDouble(rowIndex, columnIndex);
		case F -> (byte) df.getFloat(rowIndex, columnIndex);
		case L -> (byte) df.getLong(rowIndex, columnIndex);
		case I -> (byte) df.getInt(rowIndex, columnIndex);
		case T -> (byte) df.getShort(rowIndex, columnIndex);
		case Y -> df.getByte(rowIndex, columnIndex);
		default -> throw new SQLException("cannot convert from " + type + " to byte");
		};
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return getByte0(columnIndex - 1);
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte0(df.columnIndex(columnLabel));
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBytes");
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getBytes");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getCharacterStream");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getCharacterStream");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getClob");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getClob");
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLFeatureNotSupportedException("getCursorName");
	}

	private Date getDate0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return null;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case DT -> Date.valueOf(df.getDateTime(rowIndex, columnIndex).toLocalDate());
		case DA -> Date.valueOf(df.getDate(rowIndex, columnIndex));
		default -> throw new SQLException("cannot convert from " + type + " to Date");
		};
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return getDate0(columnIndex - 1);
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate0(df.columnIndex(columnLabel));
	}

	@Override
	public Date getDate(int arg0, Calendar arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getDate(int, Calendar)");
	}

	@Override
	public Date getDate(String arg0, Calendar arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getDate(String, Calendar)");
	}

	private double getDouble0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return 0d;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case BD -> df.getBigDecimal(rowIndex, columnIndex).doubleValue();
		case S, NS, FS -> Double.parseDouble(df.getString(rowIndex, columnIndex));
		case D -> df.getDouble(rowIndex, columnIndex);
		case F -> df.getFloat(rowIndex, columnIndex);
		case L -> df.getLong(rowIndex, columnIndex);
		case I -> df.getInt(rowIndex, columnIndex);
		case T -> df.getShort(rowIndex, columnIndex);
		case Y -> df.getByte(rowIndex, columnIndex);
		default -> throw new SQLException("cannot convert from " + type + " to double");
		};
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return getDouble0(columnIndex - 1);
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble0(df.columnIndex(columnLabel));
	}

	@Override
	public int getFetchDirection() throws SQLException {
		checkClosed();
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public int getFetchSize() throws SQLException {
		return Integer.MAX_VALUE;
	}

	private float getFloat0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return 0f;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case BD -> df.getBigDecimal(rowIndex, columnIndex).floatValue();
		case S, NS, FS -> Float.parseFloat(df.getString(rowIndex, columnIndex));
		case D -> (float) df.getDouble(rowIndex, columnIndex);
		case F -> df.getFloat(rowIndex, columnIndex);
		case L -> df.getLong(rowIndex, columnIndex);
		case I -> df.getInt(rowIndex, columnIndex);
		case T -> df.getShort(rowIndex, columnIndex);
		case Y -> df.getByte(rowIndex, columnIndex);
		default -> throw new SQLException("cannot convert from " + type + " to float");
		};
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return getFloat0(columnIndex - 1);
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat0(df.columnIndex(columnLabel));
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("getHoldability");
	}

	private int getInt0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return 0;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case BD -> df.getBigDecimal(rowIndex, columnIndex).intValue();
		case S, NS, FS -> Integer.parseInt(df.getString(rowIndex, columnIndex));
		case D -> (int) df.getDouble(rowIndex, columnIndex);
		case F -> (int) df.getFloat(rowIndex, columnIndex);
		case L -> (int) df.getLong(rowIndex, columnIndex);
		case I -> df.getInt(rowIndex, columnIndex);
		case T -> df.getShort(rowIndex, columnIndex);
		case Y -> df.getByte(rowIndex, columnIndex);
		default -> throw new SQLException("cannot convert from " + type + " to int");
		};
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return getInt0(columnIndex - 1);
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt0(df.columnIndex(columnLabel));
	}

	private long getLong0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return 0l;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case BD -> df.getBigDecimal(rowIndex, columnIndex).longValue();
		case S, NS, FS -> Long.parseLong(df.getString(rowIndex, columnIndex));
		case D -> (long) df.getDouble(rowIndex, columnIndex);
		case F -> (long) df.getFloat(rowIndex, columnIndex);
		case L -> df.getLong(rowIndex, columnIndex);
		case I -> df.getInt(rowIndex, columnIndex);
		case T -> df.getShort(rowIndex, columnIndex);
		case Y -> df.getByte(rowIndex, columnIndex);
		default -> throw new SQLException("cannot convert from " + type + " to long");
		};
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return getLong0(columnIndex - 1);
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong0(df.columnIndex(columnLabel));
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		return new DataFrameResultSetMetaData(df);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getNCharacterStream");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getNCharacterStream");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getNClob");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getNClob");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}

	private Object getObject0(int columnIndex) throws SQLException {
		checkClosed();
		Object o = df.get(rowIndex(), columnIndex);
		wasNull = o == null;
		return o;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getObject0(columnIndex - 1);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getObject0(df.columnIndex(columnLabel));
	}

	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getObject(int, Map)");
	}

	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getObject(String, Map)");
	}

	@Override
	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getObject(int, Class)");
	}

	@Override
	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getObject(String, Class)");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getRef");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getRef");
	}

	@Override
	public int getRow() throws SQLException {
		checkClosed();
		return row;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getRowId");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getRowId");
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getSQLXML");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getSQLXML");
	}

	private short getShort0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return 0;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case BD -> df.getBigDecimal(rowIndex, columnIndex).shortValue();
		case S, NS, FS -> Short.parseShort(df.getString(rowIndex, columnIndex));
		case D -> (short) df.getDouble(rowIndex, columnIndex);
		case F -> (short) df.getFloat(rowIndex, columnIndex);
		case L -> (short) df.getLong(rowIndex, columnIndex);
		case I -> (short) df.getInt(rowIndex, columnIndex);
		case T -> df.getShort(rowIndex, columnIndex);
		case Y -> df.getByte(rowIndex, columnIndex);
		default -> throw new SQLException("cannot convert from " + type + " to short");
		};
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return getShort0(columnIndex - 1);
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort0(df.columnIndex(columnLabel));
	}

	@Override
	public Statement getStatement() throws SQLException {
		checkClosed();
		return null;
	}

	private String getString0(int columnIndex) throws SQLException {
		checkClosed();
		Object o = df.get(rowIndex(), columnIndex);
		if (o == null) {
			wasNull = true;
			return null;
		} else {
			wasNull = false;
			return o.toString();
		}
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return getString0(columnIndex - 1);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString0(df.columnIndex(columnLabel));
	}

	private Time getTime0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return null;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case DT -> Time.valueOf(df.getDateTime(rowIndex, columnIndex).toLocalTime());
		case TI -> Time.valueOf(df.getTime(rowIndex, columnIndex));
		default -> throw new SQLException("cannot convert from " + type + " to Date");
		};
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return getTime0(columnIndex - 1);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime0(df.columnIndex(columnLabel));
	}

	@Override
	public Time getTime(int arg0, Calendar arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getTime(int, Calendar)");
	}

	@Override
	public Time getTime(String arg0, Calendar arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getTime(String, Calendar)");
	}

	private Timestamp getTimestamp0(int columnIndex) throws SQLException {

		final int rowIndex = rowIndex();

		if (wasNull = df.isNull(rowIndex, columnIndex))
			return null;

		ColumnTypeCode type = df.columnType(columnIndex).getCode();
		return switch (type) {
		case DT -> Timestamp.valueOf(df.getDateTime(rowIndex, columnIndex));
		case DA -> Timestamp.valueOf(df.getDate(rowIndex, columnIndex).atStartOfDay());
		case TI -> Timestamp.valueOf(df.getTime(rowIndex, columnIndex).atDate(LocalDate.now()));
		default -> throw new SQLException("cannot convert from " + type + " to Timestamp");
		};
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getTimestamp0(columnIndex - 1);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp0(df.columnIndex(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getTimestamp(int, Calendar)");
	}

	@Override
	public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("getTimestamp(String, Calendar)");
	}

	@Override
	public int getType() throws SQLException {
		checkClosed();
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getURL");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("getURL");
	}

	@Override
	public InputStream getUnicodeStream(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException("getUnicodeStream");
	}

	@Override
	public InputStream getUnicodeStream(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException("getUnicodeStream");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public void insertRow() throws SQLException {
		throwReadOnly();
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		checkClosed();
		return row == AFTER_LAST;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		checkClosed();
		return row == BEFORE_FIRST;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return df == null;
	}

	@Override
	public boolean isFirst() throws SQLException {
		checkClosed();
		return df.isEmpty() ? false : row == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		checkClosed();
		return df.isEmpty() ? false : row == df.size();
	}

	@Override
	public boolean last() throws SQLException {
		return absolute(df.size());
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throwReadOnly();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throwReadOnly();
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();
		if (row == AFTER_LAST)
			throw new SQLException("cursor is already after last row");
		else
			return ++row < AFTER_LAST;
	}

	@Override
	public boolean previous() throws SQLException {
		checkClosed();
		if (row == BEFORE_FIRST)
			throw new SQLException("cursor is already before first row");
		else
			return --row > BEFORE_FIRST;
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("refreshRow");
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		checkClosed();

		if (rows == 0)
			return row != BEFORE_FIRST && row != AFTER_LAST;
		else if (rows > 0) {
			int r = row + rows;
			if (r >= AFTER_LAST) {
				row = AFTER_LAST;
				return false;
			} else {
				row = r;
				return true;
			}
		} else /* rows < 0 */ {
			int r = row - rows;
			if (r <= BEFORE_FIRST) {
				row = BEFORE_FIRST;
				return false;
			} else {
				row = r;
				return true;
			}
		}
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		checkClosed();
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		checkClosed();
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		checkClosed();
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		checkClosed();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		checkClosed();
	}

	@Override
	public void updateArray(int arg0, Array arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateArray(String arg0, Array arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBlob(int arg0, Blob arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBlob(String arg0, Blob arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBlob(int arg0, InputStream arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBlob(String arg0, InputStream arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBoolean(int arg0, boolean arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBoolean(String arg0, boolean arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateByte(int arg0, byte arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateByte(String arg0, byte arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBytes(int arg0, byte[] arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateBytes(String arg0, byte[] arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateClob(int arg0, Clob arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateClob(String arg0, Clob arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateClob(int arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateClob(String arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateDate(int arg0, Date arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateDate(String arg0, Date arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateDouble(int arg0, double arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateDouble(String arg0, double arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateFloat(int arg0, float arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateFloat(String arg0, float arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateInt(int arg0, int arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateInt(String arg0, int arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateLong(int arg0, long arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateLong(String arg0, long arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNClob(int arg0, NClob arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNClob(String arg0, NClob arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNClob(int arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNClob(String arg0, Reader arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNString(int arg0, String arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNString(String arg0, String arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateObject(int arg0, Object arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateObject(String arg0, Object arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateRef(int arg0, Ref arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateRef(String arg0, Ref arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateRow() throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateRowId(int arg0, RowId arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateRowId(String arg0, RowId arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateShort(int arg0, short arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateShort(String arg0, short arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateString(int arg0, String arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateString(String arg0, String arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateTime(int arg0, Time arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateTime(String arg0, Time arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
		throwReadOnly();
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkClosed();
		return wasNull;
	}

}
