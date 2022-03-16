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

import static java.lang.Character.isLetterOrDigit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static tech.bitey.dataframe.Pr.checkArgument;
import static tech.bitey.dataframe.Pr.checkNotNull;
import static tech.bitey.dataframe.Pr.checkState;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Configuration for reading a dataframe from a CSV file.
 * 
 * @author biteytech@protonmail.com
 */
public record ReadCsvConfig(
		/**
		 * {@link ColumnType column types}. The size of this list must match the number
		 * of fields in the CSV file.
		 */
		List<ColumnType<?>> columnTypes,

		/**
		 * Must be specified if and only if a header line is not present in the CSV
		 * file.
		 */
		List<String> columnNames,

		/**
		 * Optional, defaults to using {@link ColumnType#parse(String)} for each column.
		 * Elements can be null, in which case the default parsing will be applied for
		 * the corresponding column.
		 */
		List<Function<String, Comparable<?>>> columnParsers,

		/**
		 * Any ASCII character which is not a letter, digit, double quote, CR, or LF.
		 * Defaults to comma.
		 */
		char delim,

		/**
		 * Value in CSV file to be interpreted as {@code null}. Defaults to empty
		 * string.
		 */

		String nullValue) {

	private static final char DEFAULT_DELIM = ',';
	private static final String DEFAULT_NULL_VALUE = "";

	public ReadCsvConfig {

		Pr.checkArgument(columnTypes != null && !columnTypes.isEmpty(), "columnTypes cannot be null or empty");

		if (columnNames != null)
			checkArgument(columnNames.size() == columnTypes.size(),
					"columnTypes and columnNames must have the same length");

		if (columnParsers != null)
			checkArgument(columnParsers.size() == columnTypes.size(),
					"columnParsers and columnNames must have the same length");

		checkArgument(delim <= 0x7F && !isLetterOrDigit(delim) && delim != '"' && delim != '\r' && delim != '\n',
				"delimiter must an ASCII character which is not a letter, digit, double quote, CR, or LF");

		Pr.checkNotNull(nullValue, "nullValue cannot itself be null");

		columnTypes = List.copyOf(columnTypes);
		columnNames = columnNames == null ? null : List.copyOf(columnNames);
		columnParsers = columnParsers == null ? null : new ArrayList<>(columnParsers);
	}

	public ReadCsvConfig(ColumnType<?>... columnTypes) {
		this(Arrays.asList(columnTypes), null, null, DEFAULT_DELIM, DEFAULT_NULL_VALUE);
	}

	public ReadCsvConfig(List<ColumnType<?>> columnTypes) {
		this(columnTypes, null, null, DEFAULT_DELIM, DEFAULT_NULL_VALUE);
	}

	public ReadCsvConfig withColumnNames(List<String> columnNames) {
		return new ReadCsvConfig(columnTypes, columnNames, columnParsers, delim, nullValue);
	}

	public ReadCsvConfig withColumnParsers(List<Function<String, Comparable<?>>> columnParsers) {
		return new ReadCsvConfig(columnTypes, columnNames, columnParsers, delim, nullValue);
	}

	public ReadCsvConfig withDelim(char delim) {
		return new ReadCsvConfig(columnTypes, columnNames, columnParsers, delim, nullValue);
	}

	public ReadCsvConfig withNullValue(String nullValue) {
		return new ReadCsvConfig(columnTypes, columnNames, columnParsers, delim, nullValue);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	DataFrame process(InputStream is) throws IOException {

		final ColumnBuilder[] builders = columnTypes.stream().map(ColumnType::builder).toArray(ColumnBuilder[]::new);
		final String[] columnNames;

		try (BufferedReader in = new BufferedReader(new InputStreamReader(is, UTF_8))) {

			int[] lineno = { 0 };
			int rno = 1;

			if (this.columnNames == null) {
				columnNames = nextRecord(in, rno, lineno, true);
				rno++;

				checkNotNull(columnNames, "missing header - no column names configured and empty input");
				checkState(columnNames.length == builders.length, "mismatch between number of fields in header ("
						+ columnNames.length + "), vs configured types (" + builders.length + ")");
			} else {
				columnNames = this.columnNames.toArray(new String[0]);
			}

			final Function<String, Comparable<?>>[] columnParsers = this.columnParsers == null
					? new Function[columnTypes.size()]
					: this.columnParsers.toArray(new Function[0]);
			for (int i = 0; i < columnParsers.length; i++)
				if (columnParsers[i] == null) {
					ColumnType<?> ct = columnTypes.get(i);
					columnParsers[i] = s -> (Comparable) ct.parse(s);
				}

			for (String[] fields; (fields = nextRecord(in, rno, lineno, false)) != null; rno++) {
				try {
					checkState(fields.length == builders.length, "mismatch between number of fields (" + fields.length
							+ "), vs configured types (" + builders.length + ")");

					for (int i = 0; i < fields.length; i++) {
						try {
							if (fields[i] == null)
								builders[i].addNull();
							else {
								Comparable value = columnParsers[i].apply(fields[i]);
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

	private String[] nextRecord(BufferedReader in, int rno, int lineno[], boolean header) {
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
					return split(line, header);
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

			return split(builder.toString(), header);
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

	private String[] split(String record, boolean header) {

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
			if ("\"\"".equals(fields[i]) && (header || columnTypes.get(i) == ColumnType.STRING)) {
				fields[i] = "";
				continue;
			}

			if (fields[i].startsWith("\""))
				fields[i] = fields[i].substring(1, fields[i].length() - 1);

			if (nullValue.equals(fields[i])) {
				fields[i] = null;
				continue;
			}

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
