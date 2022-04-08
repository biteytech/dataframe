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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

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

		ChannelDataFrameHeader dfHeader = new ChannelDataFrameHeader(channel);
		final int cc = dfHeader.getColumnCount();

		String[] columnNames = new String[cc];
		ColumnType<?>[] columnTypes = new ColumnType[cc];
		int[] characteristics = new int[cc];
		for (int i = 0; i < cc; i++) {
			ChannelColumnHeader columnHeader = new ChannelColumnHeader(channel);
			columnNames[i] = columnHeader.getColumnName();
			columnTypes[i] = columnHeader.getColumnType();
			characteristics[i] = columnHeader.getCharacteristics();
		}

		Column<?>[] columns = new Column<?>[cc];
		for (int i = 0; i < cc; i++)
			columns[i] = columnTypes[i].readFrom(channel, characteristics[i], dfHeader.getVersion());

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

	/**
	 * Returns a dataframe with a single column
	 * 
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * 
	 * @return a dataframe with a single column
	 */
	public static DataFrame of(String name1, Column<?> column1) {
		return create(new Column<?>[] { column1 }, new String[] { name1 });
	}

	/**
	 * Returns a dataframe with two columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 *
	 * 
	 * @return a dataframe with two columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2) {
		return create(new Column<?>[] { column1, column2 }, new String[] { name1, name2 });
	}

	/**
	 * Returns a dataframe with three columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 * @param name3   - the third column name
	 * @param column3 - the third column
	 *
	 * 
	 * @return a dataframe with three columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3) {
		return create(new Column<?>[] { column1, column2, column3 }, new String[] { name1, name2, name3 });
	}

	/**
	 * Returns a dataframe with four columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 * @param name3   - the third column name
	 * @param column3 - the third column
	 * @param name4   - the fourth column name
	 * @param column4 - the fourth column
	 *
	 * 
	 * @return a dataframe with four columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3, String name4, Column<?> column4) {
		return create(new Column<?>[] { column1, column2, column3, column4 },
				new String[] { name1, name2, name3, name4 });
	}

	/**
	 * Returns a dataframe with five columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 * @param name3   - the third column name
	 * @param column3 - the third column
	 * @param name4   - the fourth column name
	 * @param column4 - the fourth column
	 * @param name5   - the fifth column name
	 * @param column5 - the fifth column
	 *
	 * 
	 * @return a dataframe with five columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3, String name4, Column<?> column4, String name5, Column<?> column5) {
		return create(new Column<?>[] { column1, column2, column3, column4, column5 },
				new String[] { name1, name2, name3, name4, name5 });
	}

	/**
	 * Returns a dataframe with six columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 * @param name3   - the third column name
	 * @param column3 - the third column
	 * @param name4   - the fourth column name
	 * @param column4 - the fourth column
	 * @param name5   - the fifth column name
	 * @param column5 - the fifth column
	 * @param name6   - the sixth column name
	 * @param column6 - the sixth column
	 *
	 * 
	 * @return a dataframe with six columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3, String name4, Column<?> column4, String name5, Column<?> column5, String name6,
			Column<?> column6) {
		return create(new Column<?>[] { column1, column2, column3, column4, column5, column6 },
				new String[] { name1, name2, name3, name4, name5, name6 });
	}

	/**
	 * Returns a dataframe with seven columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 * @param name3   - the third column name
	 * @param column3 - the third column
	 * @param name4   - the fourth column name
	 * @param column4 - the fourth column
	 * @param name5   - the fifth column name
	 * @param column5 - the fifth column
	 * @param name6   - the sixth column name
	 * @param column6 - the sixth column
	 * @param name7   - the seventh column name
	 * @param column7 - the seventh column
	 *
	 * 
	 * @return a dataframe with seven columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3, String name4, Column<?> column4, String name5, Column<?> column5, String name6,
			Column<?> column6, String name7, Column<?> column7) {
		return create(new Column<?>[] { column1, column2, column3, column4, column5, column6, column7 },
				new String[] { name1, name2, name3, name4, name5, name6, name7 });
	}

	/**
	 * Returns a dataframe with eight columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 * @param name3   - the third column name
	 * @param column3 - the third column
	 * @param name4   - the fourth column name
	 * @param column4 - the fourth column
	 * @param name5   - the fifth column name
	 * @param column5 - the fifth column
	 * @param name6   - the sixth column name
	 * @param column6 - the sixth column
	 * @param name7   - the seventh column name
	 * @param column7 - the seventh column
	 * @param name8   - the eighth column name
	 * @param column8 - the eighth column
	 *
	 * 
	 * @return a dataframe with eight columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3, String name4, Column<?> column4, String name5, Column<?> column5, String name6,
			Column<?> column6, String name7, Column<?> column7, String name8, Column<?> column8) {
		return create(new Column<?>[] { column1, column2, column3, column4, column5, column6, column7, column8 },
				new String[] { name1, name2, name3, name4, name5, name6, name7, name8 });
	}

	/**
	 * Returns a dataframe with nine columns
	 *
	 * @param name1   - the first column name
	 * @param column1 - the first column
	 * @param name2   - the second column name
	 * @param column2 - the second column
	 * @param name3   - the third column name
	 * @param column3 - the third column
	 * @param name4   - the fourth column name
	 * @param column4 - the fourth column
	 * @param name5   - the fifth column name
	 * @param column5 - the fifth column
	 * @param name6   - the sixth column name
	 * @param column6 - the sixth column
	 * @param name7   - the seventh column name
	 * @param column7 - the seventh column
	 * @param name8   - the eighth column name
	 * @param column8 - the eighth column
	 * @param name9   - the ninth column name
	 * @param column9 - the ninth column
	 *
	 * 
	 * @return a dataframe with nine columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3, String name4, Column<?> column4, String name5, Column<?> column5, String name6,
			Column<?> column6, String name7, Column<?> column7, String name8, Column<?> column8, String name9,
			Column<?> column9) {
		return create(
				new Column<?>[] { column1, column2, column3, column4, column5, column6, column7, column8, column9 },
				new String[] { name1, name2, name3, name4, name5, name6, name7, name8, name9 });
	}

	/**
	 * Returns a dataframe with ten columns
	 *
	 * @param name1    - the first column name
	 * @param column1  - the first column
	 * @param name2    - the second column name
	 * @param column2  - the second column
	 * @param name3    - the third column name
	 * @param column3  - the third column
	 * @param name4    - the fourth column name
	 * @param column4  - the fourth column
	 * @param name5    - the fifth column name
	 * @param column5  - the fifth column
	 * @param name6    - the sixth column name
	 * @param column6  - the sixth column
	 * @param name7    - the seventh column name
	 * @param column7  - the seventh column
	 * @param name8    - the eighth column name
	 * @param column8  - the eighth column
	 * @param name9    - the ninth column name
	 * @param column9  - the ninth column
	 * @param name10   - the tenth column name
	 * @param column10 - the tenth column
	 *
	 * 
	 * @return a dataframe with ten columns
	 */
	public static DataFrame of(String name1, Column<?> column1, String name2, Column<?> column2, String name3,
			Column<?> column3, String name4, Column<?> column4, String name5, Column<?> column5, String name6,
			Column<?> column6, String name7, Column<?> column7, String name8, Column<?> column8, String name9,
			Column<?> column9, String name10, Column<?> column10) {
		return create(
				new Column<?>[] { column1, column2, column3, column4, column5, column6, column7, column8, column9,
						column10 },
				new String[] { name1, name2, name3, name4, name5, name6, name7, name8, name9, name10 });
	}

	/**
	 * Converts a {@link Collection collection} of {@link Record records} into a
	 * {@link DataFrame dataframe}, where each row in the resulting dataframe
	 * corresponds to one record in the collection (preserving iteration order). The
	 * columns in the dataframe will have the same name, order, and type as the
	 * components of the record.
	 * 
	 * @param <R>     - the record type
	 * 
	 * @param records - a collection of records. Must contain at least one non-null
	 *                value.
	 * 
	 * @return a dataframe where each row in the resulting dataframe corresponds to
	 *         one record in the collection
	 */
	@SuppressWarnings("unchecked")
	public static <R extends Record> DataFrame of(Collection<R> records) {

		Objects.requireNonNull(records, "records cannot be null");

		Class<R> clazz = null;

		for (R r : records) {
			if (r != null) {
				clazz = (Class<R>) r.getClass();
				break;
			}
		}

		Objects.requireNonNull(clazz, "records must contain at least one non-null element");

		return of(clazz, records);
	}

	/**
	 * Converts a {@link Collection collection} of {@link Record records} into a
	 * {@link DataFrame dataframe}, where each row in the resulting dataframe
	 * corresponds to one record in the collection (preserving iteration order). The
	 * columns in the dataframe will have the same name, order, and type as the
	 * components of the record.
	 * 
	 * @param <R>     - the record type
	 *
	 * @param clazz   - the record's class
	 * @param records - a collection of records
	 * 
	 * @return a dataframe where each row in the resulting dataframe corresponds to
	 *         one record in the collection
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <R extends Record> DataFrame of(Class<R> clazz, Collection<R> records) {

		Objects.requireNonNull(clazz, "clazz cannot be null");
		Objects.requireNonNull(records, "records cannot be null");

		final RecordComponent[] components = clazz.getRecordComponents();

		final String[] columnNames = new String[components.length];
		final ColumnBuilder[] builders = new ColumnBuilder[components.length];
		final Method[] accessors = new Method[components.length];

		for (int i = 0; i < components.length; i++) {

			columnNames[i] = components[i].getName();
			accessors[i] = components[i].getAccessor();
			accessors[i].setAccessible(true);

			ColumnType<?> type = ColumnType.forClass(components[i].getType());

			if (type == null)
				throw new RuntimeException("unsupported component type: " + components[i]);

			builders[i] = type.builder();
		}

		for (R record : records) {
			if (record == null) {
				for (var b : builders)
					b.addNull();
			} else {
				for (int i = 0; i < builders.length; i++) {
					try {
						Object value = accessors[i].invoke(record);
						builders[i].add(value);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}
		}

		Column[] columns = new Column[builders.length];
		for (int i = 0; i < builders.length; i++)
			columns[i] = builders[i].build();

		return create(columns, columnNames);
	}

	/**
	 * Converts an array of {@link Record records} into a {@link DataFrame
	 * dataframe}, where each row in the resulting dataframe corresponds to one
	 * record in the array (preserving order). The columns in the dataframe will
	 * have the same name, order, and type as the components of the record.
	 * 
	 * @param <R>     - the record type
	 *
	 * @param clazz   - the record's class
	 * @param records - an array of records
	 * 
	 * @return a dataframe where each row in the resulting dataframe corresponds to
	 *         one record in the collection
	 */
	@SafeVarargs
	public static <R extends Record> DataFrame of(Class<R> clazz, R... records) {
		return of(clazz, Arrays.asList(records));
	}

	/**
	 * Converts an array of {@link Record records} into a {@link DataFrame
	 * dataframe}, where each row in the resulting dataframe corresponds to one
	 * record in the array (preserving order). The columns in the dataframe will
	 * have the same name, order, and type as the components of the record.
	 * 
	 * @param <R>     - the record type
	 *
	 * @param clazz   - the record's class
	 * @param records - an array of records. Must contain at least one non-null
	 *                value.
	 * 
	 * @return a dataframe where each row in the resulting dataframe corresponds to
	 *         one record in the collection
	 */
	@SafeVarargs
	public static <R extends Record> DataFrame of(R... records) {
		return of(Arrays.asList(records));
	}
}
