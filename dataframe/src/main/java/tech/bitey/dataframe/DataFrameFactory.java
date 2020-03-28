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

package tech.bitey.dataframe;

import static tech.bitey.dataframe.DfPreconditions.checkArgument;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Factory methods for creating {@link DataFrame DataFrames}.
 * 
 * @author biteytech@protonmail.com
 */
public enum DataFrameFactory {
	;

	/**
	 * Returns a {@link DataFrame} where the column names and {@link Column columns}
	 * are provided as ordered entries in {@code columnMap}, and with the specified
	 * key column name.
	 * 
	 * @param columnMap     - specifies the columns and column names
	 * @param keyColumnName - the key column name, must be present in as a key in
	 *                      {@code columnMap}. Can be null.
	 * 
	 * @return a {@code DataFrame} where the column names and {@code columns} are
	 *         provided as ordered entries in {@code columnMap}.
	 * 
	 * @throws IllegalArgumentException if:
	 *                                  <ul>
	 *                                  <li>{@code columnMap} is empty
	 *                                  <li>{@code columnMap} contains any null keys
	 *                                  or values
	 *                                  <li>all columns do not have the same size
	 *                                  <li>{@code keyColumnName} is not null and is
	 *                                  either not present as a key in
	 *                                  {@code columnMap}, or refers to a column
	 *                                  which is not a unique index
	 *                                  </ul>
	 */
	public static DataFrame create(LinkedHashMap<String, Column<?>> columnMap, String keyColumnName) {
		return new DataFrameImpl(columnMap, keyColumnName);
	}

	/**
	 * Returns a {@link DataFrame} where the column names and {@link Column columns}
	 * are provided as ordered entries in {@code columnMap}.
	 * 
	 * @param columnMap - specifies the columns and column names
	 * 
	 * @return a {@code DataFrame} where the column names and {@code columns} are
	 *         provided as ordered entries in {@code columnMap}.
	 * 
	 * @throws IllegalArgumentException if:
	 *                                  <ul>
	 *                                  <li>{@code columnMap} is empty
	 *                                  <li>{@code columnMap} contains any null keys
	 *                                  or values
	 *                                  <li>all columns do not have the same size
	 *                                  </ul>
	 */
	public static DataFrame create(LinkedHashMap<String, Column<?>> columnMap) {
		return create(columnMap, null);
	}

	/**
	 * Returns a {@link DataFrame} with the specified {@link Column columns}, column
	 * names, and key column name.
	 * 
	 * @param columns       - the columns
	 * @param columnNames   - the column names
	 * @param keyColumnName - the key column name, must be present in
	 *                      {@code columnNames}. Can be null.
	 * 
	 * @return a {@code DataFrame} with the specified {@code columns} and column
	 *         names.
	 * 
	 * @throws IllegalArgumentException if:
	 *                                  <ul>
	 *                                  <li>column names are not distinct
	 *                                  <li>mismatched number of columns vs. column
	 *                                  names
	 *                                  <li>zero columns provided
	 *                                  <li>all columns do not have the same size
	 *                                  <li>{@code keyColumnName} is not null and is
	 *                                  either not present in {@code columnNames},
	 *                                  or refers to a column which is not a unique
	 *                                  index
	 *                                  </ul>
	 */
	public static DataFrame create(Column<?>[] columns, String[] columnNames, String keyColumnName) {
		checkArgument(columns.length == columnNames.length, "columns vs column names, mismatched lengths");
		checkArgument(new HashSet<>(Arrays.asList(columnNames)).size() == columnNames.length,
				"column names must be distinct");

		LinkedHashMap<String, Column<?>> columnMap = new LinkedHashMap<>();

		for (int i = 0; i < columns.length; i++)
			columnMap.put(columnNames[i], columns[i]);

		return create(columnMap, keyColumnName);
	}

	/**
	 * Returns a {@link DataFrame} with the specified {@link Column columns} and
	 * column names.
	 * 
	 * @param columns     - the columns
	 * @param columnNames - the column names
	 * 
	 * @return a {@code DataFrame} with the specified {@code columns} and column
	 *         names.
	 * 
	 * @throws IllegalArgumentException if:
	 *                                  <ul>
	 *                                  <li>column names are not distinct
	 *                                  <li>mismatched number of columns vs. column
	 *                                  names
	 *                                  <li>zero columns provided
	 *                                  <li>all columns do not have the same size
	 *                                  </ul>
	 */
	public static DataFrame create(Column<?>[] columns, String[] columnNames) {
		return create(columns, columnNames, null);
	}

	/**
	 * Returns a {@link DataFrame} with the specified {@link Column columns}, column
	 * names, and key column name.
	 * 
	 * @param columns       - the columns
	 * @param columnNames   - the column names
	 * @param keyColumnName - the key column name, must be present in
	 *                      {@code columnNames}. Can be null.
	 * 
	 * @return a {@code DataFrame} with the specified {@code columns} and column
	 *         names.
	 * 
	 * @throws IllegalArgumentException if:
	 *                                  <ul>
	 *                                  <li>column names are not distinct
	 *                                  <li>mismatched number of columns vs. column
	 *                                  names
	 *                                  <li>zero columns provided
	 *                                  <li>all columns do not have the same size
	 *                                  <li>{@code keyColumnName} is not null and is
	 *                                  either not present in {@code columnNames},
	 *                                  or refers to a column which is not a unique
	 *                                  index
	 *                                  </ul>
	 */
	public static DataFrame create(List<Column<?>> columns, List<String> columnNames, String keyColumnName) {
		return create(columns.toArray(new Column<?>[0]), columnNames.toArray(new String[0]), keyColumnName);
	}

	/**
	 * Returns a {@link DataFrame} with the specified {@link Column columns} and
	 * column names.
	 * 
	 * @param columns     - the columns
	 * @param columnNames - the column names
	 * 
	 * @return a {@code DataFrame} with the specified {@code columns} and column
	 *         names.
	 * 
	 * @throws IllegalArgumentException if:
	 *                                  <ul>
	 *                                  <li>column names are not distinct
	 *                                  <li>mismatched number of columns vs. column
	 *                                  names
	 *                                  <li>zero columns provided
	 *                                  <li>all columns do not have the same size
	 *                                  </ul>
	 */
	public static DataFrame create(List<Column<?>> columns, List<String> columnNames) {
		return create(columns, columnNames, null);
	}

	/**
	 * Load a dataframe from a file created via {@link DataFrame#writeTo(File)}.
	 * 
	 * @param file - the file to read from
	 * 
	 * @return the dataframe loaded from the specified file
	 * 
	 * @throws IOException if some I/O error occurs
	 */
	public static DataFrame readFrom(File file) throws IOException {
		try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);) {
			return readFrom(fileChannel);
		}
	}

	/**
	 * Read a dataframe from the specified {@link ReadableByteChannel}.
	 * 
	 * @param channel - the channel to read from
	 * 
	 * @return the dataframe read from the specified channel
	 * 
	 * @throws IOException if some I/O error occurs
	 */
	public static DataFrame readFrom(ReadableByteChannel channel) throws IOException {

		FileDataFrameHeader dfHeader = new FileDataFrameHeader(channel);
		final int cc = dfHeader.getColumnCount();

		String[] columnNames = new String[cc];
		ColumnType<?>[] columnTypes = new ColumnType[cc];
		int[] characteristics = new int[cc];
		for (int i = 0; i < cc; i++) {
			FileColumnHeader columnHeader = new FileColumnHeader(channel);
			columnNames[i] = columnHeader.getColumnName();
			columnTypes[i] = columnHeader.getColumnType();
			characteristics[i] = columnHeader.getCharacteristics();
		}

		Column<?>[] columns = new Column<?>[cc];
		for (int i = 0; i < cc; i++)
			columns[i] = columnTypes[i].readFrom(channel, characteristics[i]);

		Integer keyIndex = dfHeader.keyIndex();
		return create(columns, columnNames, keyIndex == null ? null : columnNames[keyIndex]);
	}

	/**
	 * Read a dataframe from a CSV file. The CSV must adhere to
	 * <a href="https://tools.ietf.org/html/rfc4180">RFC 4180</a>, with the
	 * following exceptions:
	 * <ul>
	 * <li>Lines can end in CR, LF, or CRLF (not just CRLF as defined in the RFC)
	 * <li>If a CR or CRLF appears within a field, it will be converted to LF
	 * <li>A delimiter other than comma may be specified in the configuration
	 * <li>A header line must be present if and only if the column names are not
	 * provided in the configuration
	 * </ul>
	 * 
	 * @param file   - the file containing the CVS data
	 * @param config - configuration for parsing the CSV file. See
	 *               {@link ReadCsvConfig}.
	 * @return the dataframe read from the CSV file
	 * 
	 * @throws IOException      if some I/O error occurs
	 * @throws RuntimeException if there is any other error parsing the CVS file
	 * 
	 * @see ReadCsvConfig
	 */
	public static DataFrame readCsvFrom(File file, ReadCsvConfig config) throws IOException {
		try (FileInputStream fis = new FileInputStream(file)) {
			return readCsvFrom(fis, config);
		}
	}

	/**
	 * Read a dataframe in CSV format from the specified {@link InputStream}. The
	 * CSV must adhere to <a href="https://tools.ietf.org/html/rfc4180">RFC
	 * 4180</a>, with the following exceptions:
	 * <ul>
	 * <li>Lines can end in CR, LF, or CRLF (not just CRLF as defined in the RFC)
	 * <li>If a CR or CRLF appears within a field, it will be converted to LF
	 * <li>A delimiter other than comma may be specified in the configuration
	 * <li>A header line must be present if and only if the column names are not
	 * provided in the configuration
	 * </ul>
	 * 
	 * @param is     - the {@code InputStream} containing the CSV data
	 * @param config - configuration for parsing the CSV file. See
	 *               {@link ReadCsvConfig}.
	 * @return the dataframe read from the CSV file
	 * 
	 * @throws IOException      if some I/O error occurs
	 * @throws RuntimeException if there is any other error parsing the CVS file
	 * 
	 * @see ReadCsvConfig
	 */
	public static DataFrame readCsvFrom(InputStream is, ReadCsvConfig config) throws IOException {
		return config.process(is);
	}

	/**
	 * Read a dataframe from the specified {@link ResultSet} according to the
	 * specified {@link ReadFromDbConfig configuration}.
	 * 
	 * @param rs     - a {@link ResultSet}
	 * @param config - a {@link ReadFromDbConfig configuration} containing
	 *               per-column logic for getting fields from the {@code ResultSet}.
	 *               The number of columns in the configuration must match the
	 *               number in the {@code ResultSet}. Configurations can be reused
	 *               for multiple {@code ResultSets}.
	 * 
	 * @return the dataframe read from the {@code ResultSet}
	 * 
	 * @throws SQLException          if some SQL or database error occurs
	 * @throws IllegalStateException if the number of columns in the configuration
	 *                               does not match the number in the
	 *                               {@code ResultSet}
	 */
	public static DataFrame readFrom(ResultSet rs, ReadFromDbConfig config) throws SQLException {
		return config.read(rs);
	}
}
