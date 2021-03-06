/*
 * Copyright 2021 biteytech@protonmail.com
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

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.db.DateFromResultSet;
import tech.bitey.dataframe.db.DoubleFromResultSet;
import tech.bitey.dataframe.db.IntFromResultSet;
import tech.bitey.dataframe.db.StringFromResultSet;

public class SampleUsages {

	/**
	 * Creating {@link Column columns}.
	 */
	@Test
	public void ex0() {

		IntColumn c1 = IntColumn.of(1, 2, null, 3, null, null, 5);
		IntColumn c2 = IntColumn.builder().addAll(1, 2).addNull().add(3).addNulls(2).add(5).build();

		Assertions.assertEquals(c1, c2);
	}

	/**
	 * Converting arrays and collections to {@link Column columns}.
	 */
	@Test
	public void ex1() {
		// primitive arrays
		{
			int[] a = { 5, 1, 3 };

			IntColumn c = IntColumn.builder().addAll(a).build();

			Assertions.assertEquals(a[1], c.getInt(1));
			Assertions.assertArrayEquals(a, c.intStream().toArray());
		}

		// Object arrays
		{
			Integer[] a = { 5, 1, 3 };
			String[] b = { "e", "a", "c" };

			IntColumn c1 = IntColumn.of(a);
			StringColumn c2 = StringColumn.of(b);

			Assertions.assertEquals(a[1], c1.get(1));
			Assertions.assertEquals(b[1], c2.get(1));

			Assertions.assertArrayEquals(a, c1.toArray());
			Assertions.assertArrayEquals(b, c2.toArray());
		}

		// Collections
		{
			List<Integer> a = Arrays.asList(new Integer[] { 5, 1, 3 });
			List<String> b = Arrays.asList(new String[] { "e", "a", "c" });

			IntColumn c1 = IntColumn.of(a);
			StringColumn c2 = StringColumn.of(b);

			Assertions.assertEquals(a.get(1), c1.get(1));
			Assertions.assertEquals(b.get(1), c2.get(1));

			Assertions.assertEquals(a, c1);
			Assertions.assertEquals(b, c2);
		}
	}

	/**
	 * Sorting {@link Column columns}.
	 */
	@Test
	public void ex2() {

		List<String> l = Arrays.asList(new String[] { "c", "b", "a", "c" });
		StringColumn s = StringColumn.of(l);

		Assertions.assertEquals(l, s);

		Collections.sort(l);
		Assertions.assertEquals(l, s.toSorted());
		Assertions.assertEquals(l.subList(0, 3), s.toDistinct());
	}

	/**
	 * {@link Column Column} conversions.
	 */
	@Test
	public void ex3() {

		StringColumn s = StringColumn.of("1", "2", null, "3");

		Assertions.assertEquals(s, s.parseInt().toStringColumn());
		Assertions.assertEquals(s, s.toIntColumn(Integer::parseInt).toStringColumn(Object::toString));
	}

	/**
	 * Creating {@link DataFrame DataFrames}.
	 */
	@Test
	public void ex4() throws Exception {
		// DataFrame from Columns
		IntColumn c1 = IntColumn.of(100, 200, 300);
		DateColumn c2 = DateColumn.of(LocalDate.of(2019, 1, 6), LocalDate.of(2019, 1, 23), LocalDate.of(2019, 2, 9));
		StringColumn c3 = StringColumn.of("Jones", null, "Gill");
		IntColumn c4 = IntColumn.of(95, 50, 36);
		DoubleColumn c5 = DoubleColumn.of(1.99, 19.99, 4.99);

		DataFrame df = DataFrameFactory.create(new Column<?>[] { c1, c2, c3, c4, c5 },
				new String[] { "ORDER_ID", "ORDER_DATE", "SALESPERSON", "UNITS", "UNIT_COST" });

		DataFrame df0 = DataFrameConfig.builder().columns(c1, c2, c3, c4, c5)
				.columnNames("ORDER_ID", "ORDER_DATE", "SALESPERSON", "UNITS", "UNIT_COST").build().create();

		Assertions.assertEquals(df, df0);

		// DataFrame from csv
		StringBuilder csv = new StringBuilder();
		csv.append("ORDER_ID	ORDER_DATE	SALESPERSON	UNITS	UNIT_COST\n");
		csv.append("100	1/6/2019	Jones	95	1.99\n");
		csv.append("200	1/23/2019		50	19.99\n");
		csv.append("300	2/9/2019	Gill	36	4.99\n");

		File file = File.createTempFile("ex4", null);
		file.deleteOnExit();
		Files.write(file.toPath(), csv.toString().getBytes());

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
		ReadCsvConfig config = ReadCsvConfig.builder()
				.columnTypes(ColumnType.INT, ColumnType.DATE, ColumnType.STRING, ColumnType.INT, ColumnType.DOUBLE)
				.columnParsers(null, text -> LocalDate.parse(text, formatter), null, null, null).delim('\t').build();

		DataFrame df2 = DataFrameFactory.readCsvFrom(file, config);

		Assertions.assertEquals(df, df2);

		// DataFrame from SQL query
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
				Statement stmt = conn.createStatement();) {

			stmt.execute(
					"create table EX4 (ORDER_ID INT, ORDER_DATE TEXT, SALESPERSON TEXT, UNITS INT, UNIT_COST DOUBLE)");

			PreparedStatement ps = conn.prepareStatement("insert into EX4 values (?,?,?,?,?)");
			ex4Insert(ps, 100, LocalDate.of(2019, 1, 6), "Jones", 95, 1.99);
			ex4Insert(ps, 200, LocalDate.of(2019, 1, 23), null, 50, 19.99);
			ex4Insert(ps, 300, LocalDate.of(2019, 2, 9), "Gill", 36, 4.99);

			ResultSet rs = stmt.executeQuery("select * from EX4");
			DataFrame df3 = DataFrameFactory.readFrom(rs,
					ReadFromDbConfig.builder()
							.fromRsLogic(IntFromResultSet.INT_FROM_INT, DateFromResultSet.DATE_FROM_ISOSTRING,
									StringFromResultSet.STRING_FROM_STRING, IntFromResultSet.INT_FROM_INT,
									DoubleFromResultSet.DOUBLE_FROM_DOUBLE)
							.build());

			Assertions.assertEquals(df, df3);
		}
	}

	/**
	 * Processing {@link DataFrame DataFrames}.
	 */
	@Test
	public void ex5() {

		IntColumn c1 = IntColumn.of(100, 200, 300);
		DateColumn c2 = DateColumn.of(LocalDate.of(2019, 1, 6), LocalDate.of(2019, 1, 23), LocalDate.of(2019, 2, 9));
		StringColumn c3 = StringColumn.of("Jones", null, "Gill");
		IntColumn c4 = IntColumn.of(95, 50, 36);
		DoubleColumn c5 = DoubleColumn.of(1.99, 19.99, 4.99);
		DataFrame df = DataFrameFactory.create(new Column<?>[] { c1, c2, c3, c4, c5 },
				new String[] { "ORDER_ID", "ORDER_DATE", "SALESPERSON", "UNITS", "UNIT_COST" });

		// streaming Rows
		Map<Month, Double> map1 = df.stream().collect(Collectors.groupingBy(row -> row.getDate("ORDER_DATE").getMonth(),
				Collectors.summingDouble(row -> row.getInt("UNITS") * row.getDouble("UNIT_COST"))));

		// using a Cursor
		Map<Month, Double> map2 = new HashMap<>();
		for (Cursor c = df.cursor(); c.hasNext(); c.next()) {
			LocalDate date = c.getDate("ORDER_DATE");
			double total = c.getInt("UNITS") * c.getDouble("UNIT_COST");

			map2.merge(date.getMonth(), total, (t1, t2) -> t1 + t2);
		}

		Assertions.assertEquals(map1.keySet(), map2.keySet());
		map1.keySet().forEach(key -> Assertions.assertEquals(map1.get(key), map2.get(key), 0.0000001));
	}

	private static void ex4Insert(PreparedStatement ps, int c1, LocalDate c2, String c3, int c4, double c5)
			throws Exception {
		int i = 1;
		ps.setInt(i++, c1);
		ps.setString(i++, c2.toString());
		ps.setString(i++, c3);
		ps.setInt(i++, c4);
		ps.setDouble(i++, c5);
		ps.executeUpdate();
	}
}
