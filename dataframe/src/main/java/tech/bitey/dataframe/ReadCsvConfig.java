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

import static java.lang.Character.isLetterOrDigit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.guava.DfPreconditions.checkNotNull;
import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Configuration for reading a dataframe from a CSV file. The three configurable
 * settings are:
 * <ul>
 * <li>Column types - this is mandatory as there is currently no logic to
 * auto-detect column types.
 * <li>Column names - required if and only if there is no header line.
 * <li>Delimiter - defaults to comma
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public class ReadCsvConfig {

	private final ColumnType[] columnTypes;
	private final String[] columnNames;
	private final char delim;

	/**
	 * Constructor for specifying all three configurable settings.
	 * 
	 * @param columnTypes - mandatory {@link ColumnType column types}. The size of
	 *                    this list must match the number of fields in the CSV file.
	 * @param columnNames - must be specified if and only if a header line is not
	 *                    present in the CSV file.
	 * @param delim       - any ASCII character which is not a letter, digit, double
	 *                    quote, CR, or LF.
	 */
	public ReadCsvConfig(List<ColumnType> columnTypes, List<String> columnNames, char delim) {

		checkNotNull(columnTypes, "columnTypes array cannot be null");
		checkArgument(columnTypes.size() > 0, "columnTypes array cannot be empty");

		if (columnNames != null)
			checkArgument(columnNames.size() == columnTypes.size(),
					"columnTypes and columnNames must have the same length");

		checkArgument(delim <= 0x7F && !isLetterOrDigit(delim) && delim != '"' && delim != '\r' && delim != '\n',
				"delimiter must an ASCII character which is not a letter, digit, double quote, CR, or LF");

		this.columnTypes = columnTypes.toArray(new ColumnType[0]);
		this.columnNames = columnNames == null ? null : columnNames.toArray(new String[0]);
		;
		this.delim = delim;
	}

	/**
	 * Constructor for specifying the column types and delimiter settings.
	 * <p>
	 * The CSV file must contain a header line since no column names are provided.
	 * 
	 * @param columnTypes - mandatory {@link ColumnType column types}. The size of
	 *                    this list must match the number of fields in the CSV file.
	 * @param delim       - any ASCII character which is not a letter, digit, double
	 *                    quote, CR, or LF.
	 */
	public ReadCsvConfig(List<ColumnType> columnTypes, char delim) {
		this(columnTypes, null, delim);
	}

	/**
	 * Constructor for specifying the column types and and column names settings.
	 * <p>
	 * The delimiter will default to comma.
	 * 
	 * @param columnTypes - mandatory {@link ColumnType column types}. The size of
	 *                    this list must match the number of fields in the CSV file.
	 * @param columnNames - must be specified if and only if a header line is not
	 *                    present in the CSV file.
	 */
	public ReadCsvConfig(List<ColumnType> columnTypes, List<String> columnNames) {
		this(columnTypes, columnNames, ',');
	}

	/**
	 * Constructor for specifying the column types settings.
	 * <p>
	 * The CSV file must contain a header line since no column names are provided.
	 * The delimiter will default to comma.
	 * 
	 * @param columnTypes - mandatory {@link ColumnType column types}. The size of
	 *                    this list must match the number of fields in the CSV file.
	 */
	public ReadCsvConfig(List<ColumnType> columnTypes) {
		this(columnTypes, null, ',');
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	DataFrame process(InputStream is) throws IOException {

		final ColumnBuilder[] builders = Arrays.stream(columnTypes).map(ColumnType::builder)
				.toArray(ColumnBuilder[]::new);

		String[] columnNames = this.columnNames;

		try (BufferedReader in = new BufferedReader(new InputStreamReader(is, UTF_8))) {

			int[] lineno = { 0 };
			int rno = 1;

			if (columnNames == null) {
				columnNames = nextRecord(in, rno, lineno);
				rno++;

				checkNotNull(columnNames, "missing header - no column names configured and empty input");
				checkState(columnNames.length == builders.length, "mismatch between number of fields in header ("
						+ columnNames.length + "), vs configured types (" + builders.length + ")");
			}

			for (String[] fields; (fields = nextRecord(in, rno, lineno)) != null; rno++) {
				try {
					checkState(fields.length == builders.length, "mismatch between number of fields (" + fields.length
							+ "), vs configured types (" + builders.length + ")");

					for (int i = 0; i < fields.length; i++) {
						try {
							if (fields[i].isEmpty())
								builders[i].addNull();
							else {
								Object value = builders[i].getType().parse(fields[i]);
								builders[i].add(value);
							}
						} catch (Exception e) {
							throw new RuntimeException(errorMessage(i + 1, e.getMessage()), e);
						}
					}
				} catch (Exception e) {
					throw new RuntimeException(errorMessage(rno, lineno[0], e.getMessage()), e);
				}
			}
		}

		Column<?>[] columns = new Column<?>[builders.length];
		for (int i = 0; i < columns.length; i++)
			columns[i] = builders[i].build();

		return DataFrameFactory.create(columns, columnNames);
	}

	private String[] nextRecord(BufferedReader in, int rno, int lineno[]) {
		try {
			int count = 0;
			StringBuilder builder = null;

			do {
				String line = in.readLine();
				lineno[0]++;

				if (line == null) {
					if (builder == null)
						return null;
					else
						throw new IllegalStateException("reached EOF with unmatched quote");
				}

				// count double quotes
				for (int i = 0; i < line.length(); i++)
					if (line.charAt(i) == '"')
						count++;

				if (builder == null && count % 2 == 0) {
					// first line is a whole record
					return split(line);
				} else if (builder == null) {
					// first line is not a whole record
					builder = new StringBuilder();
					builder.append(line);
				} else {
					// subsequent lines always appended
					builder.append('\n');
					builder.append(line);
				}
			} while (count % 2 == 1);

			return split(builder.toString());
		} catch (Exception e) {
			throw new RuntimeException(errorMessage(rno, lineno[0], e.getMessage()), e);
		}
	}

	private static String errorMessage(int rno, int lineno, String error) {
		return String.format("Record #%d: Line #%d: %s", rno, lineno, error);
	}

	private static String errorMessage(int fno, String error) {
		return String.format("Field #%d: %s", fno, error);
	}

	private String[] split(String record) {

		List<Integer> splits = new ArrayList<>();

		for (int i = 0, count = 0; i < record.length(); i++) {

			final char c = record.charAt(i);

			if (c == '"')
				count++;
			else if (c == delim && count % 2 == 0)
				splits.add(i);
		}

		int prevSplit = -1, i = 0;
		String[] fields = new String[splits.size() + 1];
		for (int split : splits) {
			fields[i++] = record.substring(prevSplit + 1, split);
			prevSplit = split;
		}
		fields[i] = record.substring(prevSplit + 1);

		for (i = 0; i < fields.length; i++) {
			if (fields[i].startsWith("\""))
				fields[i] = fields[i].substring(1, fields[i].length() - 1);

			int count = 0;
			for (int j = 0; j < fields[i].length(); j++) {
				if (fields[i].charAt(j) == '"')
					count++;
				else if (count > 0) {
					if (count % 2 == 0)
						count = 0;
					else
						throw new RuntimeException(errorMessage(i + 1, "unescaped \""));
				}
			}

			fields[i] = fields[i].replace("\"\"", "\"");
		}

		return fields;
	}
}