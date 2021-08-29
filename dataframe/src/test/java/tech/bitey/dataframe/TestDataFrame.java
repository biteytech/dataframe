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

import static java.util.Spliterator.DISTINCT;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.db.BooleanFromResultSet;
import tech.bitey.dataframe.db.ByteFromResultSet;
import tech.bitey.dataframe.db.DateFromResultSet;
import tech.bitey.dataframe.db.DateTimeFromResultSet;
import tech.bitey.dataframe.db.DecimalFromResultSet;
import tech.bitey.dataframe.db.DoubleFromResultSet;
import tech.bitey.dataframe.db.FloatFromResultSet;
import tech.bitey.dataframe.db.IFromResultSet;
import tech.bitey.dataframe.db.IntFromResultSet;
import tech.bitey.dataframe.db.LongFromResultSet;
import tech.bitey.dataframe.db.NormalStringFromResultSet;
import tech.bitey.dataframe.db.ShortFromResultSet;
import tech.bitey.dataframe.db.StringFromResultSet;
import tech.bitey.dataframe.db.TimeFromResultSet;
import tech.bitey.dataframe.db.UuidFromResultSet;

public class TestDataFrame {

	private final Map<String, DataFrame> DF_MAP = new HashMap<>();

	@SuppressWarnings({ "rawtypes" })
	TestDataFrame() {

		Map<String, List<Column<?>>> columnTestMap = new HashMap<>();
		new TestIntColumn().samples().forEach(s -> {
			List<Column<?>> list;
			columnTestMap.put(s.toString(), list = new ArrayList<>());
			list.add(s.column());
		});

		TestColumn[] columnTest = new TestColumn[] { new TestLongColumn(), new TestFloatColumn(),
				new TestDoubleColumn(), new TestStringColumn(), new TestBooleanColumn(), new TestDecimalColumn(),
				new TestShortColumn(), new TestByteColumn(), new TestUuidColumn(), new TestNormalStringColumn() };

		for (TestColumn<?> tests : columnTest) {
			tests.samples().forEach(s -> {
				List<Column<?>> list = columnTestMap.get(s.toString());
				if (list != null && list.get(0).size() == s.size())
					list.add(s.column());
			});
		}

		for (Map.Entry<String, List<Column<?>>> e : columnTestMap.entrySet()) {

			Column<?>[] columns = e.getValue().toArray(new Column<?>[0]);
			String[] columnNames = new String[columns.length];
			for (int i = 0; i < columnNames.length; i++)
				columnNames[i] = "C" + i;

			DataFrame df = DataFrameFactory.create(columns, columnNames);
			DF_MAP.put(e.getKey(), df);
		}
	}

	@Test
	public void testCopy() {
		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame df = e.getValue();
			DataFrame copy = df.copy();

			Assertions.assertEquals(df, copy, e.getKey() + ", copy");
		}
	}

	@Test
	public void testAppend() {
		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame df = e.getValue();
			if (df.size() < 2)
				continue;

			int half = df.size() / 2;
			DataFrame head = df.subFrame(0, half);
			DataFrame tail = df.subFrame(half, df.size());

			DataFrame joint = head.append(tail);

			Assertions.assertEquals(df, joint, e.getKey() + ", append");
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCursorVsIterator() {
		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame df = e.getValue();

			final String test = e.getKey() + ", cursor and iter";

			Iterator<Row> iter = df.iterator();
			for (Cursor cursor = df.cursor(); cursor.hasNext(); cursor.next()) {
				Row row = iter.next();
				Assertions.assertEquals(cursor.columnCount(), row.columnCount(), test);
				Assertions.assertEquals(cursor.columnCount(), df.columnCount(), test);
				for (int i = 0; i < df.columnCount(); i++)
					Assertions.assertEquals(cursor.<Comparable>get(i), row.<Comparable>get(i), test);
			}
			Assertions.assertFalse(iter.hasNext(), test);
		}
	}

	@Test
	public void testHashJoin() {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame expected = e.getValue();

			if (expected.stream().collect(Collectors.toSet()).size() < expected.size())
				continue;

			String[] columnNames = expected.columnNames().stream().toArray(String[]::new);
			DataFrame actual = expected.join(expected, columnNames, columnNames);

			Assertions.assertEquals(expected, actual, e.getKey() + ", hashJoin");
		}
	}

	@Test
	public void testInnerVsHash() {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame df = e.getValue();

			if (!df.column(0).isDistinct())
				continue;

			DataFrame expected = df.join(df, new String[] { df.columnName(0) }, new String[] { df.columnName(0) });

			DataFrame actual = df.withKeyColumn(0);
			actual = actual.join(actual);

			Assertions.assertTrue(expected.equals(actual, true), e.getKey() + ", inner vs hash");
		}
	}

	@Test
	public void testReadWriteBinary() throws Exception {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame expected = e.getValue();

			File file = File.createTempFile(e.getKey(), null);
			file.deleteOnExit();

			expected.writeTo(file);
			DataFrame actual = DataFrameFactory.readFrom(file);

			Assertions.assertEquals(expected, actual, e.getKey() + ", read/write binary");
		}
	}

	@Test
	public void testReadWriteCsv() throws Exception {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame expected = e.getValue();

			File file = File.createTempFile(e.getKey(), null);
			file.deleteOnExit();

			expected.writeCsvTo(file);
			DataFrame actual = DataFrameFactory.readCsvFrom(file,
					ReadCsvConfig.builder().columnTypes(expected.columnTypes()).build());

			Assertions.assertEquals(expected, actual, e.getKey() + ", read/write csv");
		}

		// test null vs empty string
		{
			StringColumn column = StringColumn.of(null, "", "foo", "", null);
			DataFrame expected = DataFrameFactory.create(new Column<?>[] { column }, new String[] { "COLUMN" });

			File file = File.createTempFile("emptyString", "csv");
			file.deleteOnExit();
			expected.writeCsvTo(file);

			DataFrame actual = DataFrameFactory.readCsvFrom(file,
					ReadCsvConfig.builder().columnTypes(ColumnType.STRING).build());

			Assertions.assertEquals(expected, actual, "null vs empty, read/write csv");
		}
	}

	@Test
	public void testReadWriteDb() throws Exception {

		try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");
				Statement stmt = conn.createStatement();) {

			conn.setAutoCommit(false);

			int count = 0;

			for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

				String tableName = "test" + count++;

				DataFrame expected = e.getValue();
				int cc = expected.columnCount();

				StringBuilder create = new StringBuilder();
				create.append("create table ").append(tableName).append(" (");

				List<IFromResultSet<?, ?>> fromRsLogic = new ArrayList<>();

				for (int i = 0; i < cc; i++) {
					create.append(expected.columnName(i)).append(' ');
					switch (expected.columnType(i).getCode()) {
					case B:
						create.append("TEXT");
						fromRsLogic.add(BooleanFromResultSet.BOOLEAN_FROM_STRING);
						break;
					case DA:
						create.append("TEXT");
						fromRsLogic.add(DateFromResultSet.DATE_FROM_DATE);
						break;
					case DT:
						create.append("TEXT");
						fromRsLogic.add(DateTimeFromResultSet.DATETIME_FROM_TIMESTAMP);
						break;
					case TI:
						create.append("TEXT");
						fromRsLogic.add(TimeFromResultSet.TIME_FROM_STRING);
						break;
					case S:
						create.append("TEXT");
						fromRsLogic.add(StringFromResultSet.STRING_FROM_STRING);
						break;
					case BD:
						create.append("NUMERIC");
						fromRsLogic.add(DecimalFromResultSet.BIGDECIMAL_FROM_BIGDECIMAL);
						break;
					case D:
						create.append("DOUBLE");
						fromRsLogic.add(DoubleFromResultSet.DOUBLE_FROM_DOUBLE);
						break;
					case F:
						create.append("FLOAT");
						fromRsLogic.add(FloatFromResultSet.FLOAT_FROM_FLOAT);
						break;
					case I:
						create.append("INT");
						fromRsLogic.add(IntFromResultSet.INT_FROM_INT);
						break;
					case L:
						create.append("INT8");
						fromRsLogic.add(LongFromResultSet.LONG_FROM_LONG);
						break;
					case T:
						create.append("INT2");
						fromRsLogic.add(ShortFromResultSet.SHORT_FROM_SHORT);
						break;
					case Y:
						create.append("INT2");
						fromRsLogic.add(ByteFromResultSet.BYTE_FROM_BYTE);
						break;
					case UU:
						create.append("TEXT");
						fromRsLogic.add(UuidFromResultSet.UUID_FROM_STRING);
						break;
					case NS:
						create.append("TEXT");
						fromRsLogic.add(NormalStringFromResultSet.STRING_FROM_STRING);
						break;
					}

					if (i < cc - 1)
						create.append(", ");
					else
						create.append(")");
				}

				stmt.execute(create.toString());

				try (PreparedStatement ps = conn.prepareStatement(
						"insert into " + tableName + " values (?" + DfStrings.repeat(",?", cc - 1) + ")");) {

					expected.writeTo(ps, WriteToDbConfig.DEFAULT_CONFIG);

					ResultSet rs = stmt.executeQuery("select * from " + tableName);
					DataFrame actual = DataFrameFactory.readFrom(rs,
							ReadFromDbConfig.builder().fromRsLogic(fromRsLogic).build());
					Assertions.assertEquals(expected, actual, e.getKey() + ", read/write db");
				}
			}
		}
	}

	@Test
	public void testSubFrameByValue() throws Exception {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame df = e.getValue();
			if (df.isEmpty() || !df.column(0).isDistinct())
				continue;
			df = df.withKeyColumn(0);

			testSubFrameByValue(e.getKey(), df, false, false);
			testSubFrameByValue(e.getKey(), df, false, true);
			testSubFrameByValue(e.getKey(), df, true, false);
			testSubFrameByValue(e.getKey(), df, true, true);
		}
	}

	@Test
	public void testFilterNulls() throws Exception {

		StringColumn c11 = StringColumn.of(null, "B", "C");
		IntColumn c21 = IntColumn.of(1, 2, null);
		DataFrame df1 = DataFrameFactory.create(new Column<?>[] { c11, c21 }, new String[] { "C1", "C2" });
		Assertions.assertEquals(df1.subFrame(1, 2), df1.filterNulls(), "basic, filter nulls");

		Predicate<Row> nullFilter = r -> {
			for (int i = 0; i < r.columnCount(); i++)
				if (r.isNull(i))
					return false;
			return true;
		};

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame df = e.getValue();

			Assertions.assertEquals(df.filter(nullFilter), df.filterNulls(), e.getKey() + ", filter nulls");
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testSubFrameByValue(String label, DataFrame df, boolean fromInclusive, boolean toInclusive)
			throws Exception {

		Column keyColumn = df.column(0);
		Comparable from = keyColumn.first();
		Comparable to = keyColumn.last();

		Column subColumn = keyColumn.subColumnByValue(from, fromInclusive, to, toInclusive);

		DataFrame subFrame = df.subFrameByValue(from, fromInclusive, to, toInclusive);

		Assertions.assertEquals(subColumn, subFrame.column(0),
				label + ", subFrameByValue, (" + fromInclusive + "," + toInclusive + ")");
	}

	@Test
	public void testOneToManyJoin() {

		StringColumn c11 = StringColumn.builder(DISTINCT).add("A", "B", "C", "D").build();
		IntColumn c21 = IntColumn.of(1, 2, 3, 4);
		StringColumn c31 = StringColumn.of("one", "two", "three", "four");
		DataFrame df1 = DataFrameFactory.create(new Column<?>[] { c11, c21, c31 }, new String[] { "C1", "C2", "C3" },
				"C1");

		StringColumn c12 = StringColumn.of(null, "D", "A", "B", "C", "C", null, "D");
		IntColumn c22 = IntColumn.of(null, 4, 1, 2, 3, 3, null, 4);
		DataFrame df2 = DataFrameFactory.create(new Column<?>[] { c12, c22 }, new String[] { "C1", "C2" });

		DataFrame expected = df2.filterNulls();
		expected = expected.withColumn("C3", StringColumn.of("four", "one", "two", "three", "three", "four"))
				.withColumn("C2_2", expected.column(1));

		DataFrame actual = df1.joinLeftOneToMany(df2, "C1");

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testManyToOneJoin() {

		StringColumn c11 = StringColumn.of(null, "A", "B", "C", null, "D");
		IntColumn c21 = IntColumn.of(null, 1, 2, 3, null, 5);
		DataFrame df1 = DataFrameFactory.create(new Column<?>[] { c11, c21 }, new String[] { "C1", "C2" });

		StringColumn c12 = StringColumn.of("A2", "B2", "C2", "D2");
		IntColumn c22 = IntColumn.builder(DISTINCT).addAll(1, 2, 3, 4).build();
		StringColumn c32 = StringColumn.of("one", "two", "three", "four");
		DataFrame df2 = DataFrameFactory.create(new Column<?>[] { c12, c22, c32 }, new String[] { "C1", "C2", "C3" },
				"C2");

		DataFrame joint = df1.subFrame(1, 5).joinManyToOne(df2, "C2");
		DataFrame expected = DataFrameFactory.create(
				new Column<?>[] { c11.subColumn(1, 4), c22.subColumn(0, 3), c12.subColumn(0, 3), c32.subColumn(0, 3) },
				new String[] { "C1", "C2", "C1_2", "C3" });
		Assertions.assertEquals(expected, joint);
	}

	@Test
	public void testHashJoin2() {

		StringColumn c11 = StringColumn.of("A", "A", "B", "B", "C", "C");
		IntColumn c21 = IntColumn.of(1, 2, 3, 1, 2, 3);
		StringColumn c31 = StringColumn.of("foo", "foo", "foo", "foo", "foo", "foo");
		DataFrame df1 = DataFrameFactory.create(new Column<?>[] { c11, c21, c31 },
				new String[] { "KEY1", "KEY2", "FOO" });

		StringColumn c12 = StringColumn.of("B", "B", "C", "C", "D", "D");
		IntColumn c22 = IntColumn.of(2, 3, 4, 2, 3, 4);
		StringColumn c32 = StringColumn.of("bar", "bar", "bar", "bar", "bar", "bar");
		DataFrame df2 = DataFrameFactory.create(new Column<?>[] { c12, c22, c32 },
				new String[] { "KEY1", "KEY2", "BAR" });

		DataFrame actual = df1.join(df2, new String[] { "KEY1", "KEY2" }, new String[] { "KEY1", "KEY2" });

		StringColumn e1 = StringColumn.of("B", "C");
		IntColumn e2 = IntColumn.of(3, 2);
		StringColumn e3 = StringColumn.of("foo", "foo");
		StringColumn e4 = StringColumn.of("bar", "bar");
		DataFrame expected = DataFrameFactory.create(new Column<?>[] { e1, e2, e3, e4 },
				new String[] { "KEY1", "KEY2", "FOO", "BAR" });

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testAsMap() throws Exception {

		IntColumn df1KeyColumn = IntColumn.builder(DISTINCT).addAll(1, 3, 4, 5).build();
		StringColumn df1ValueColumn = StringColumn.of("one", "three", "four", "five");
		DataFrame df1 = DataFrameFactory.create(new Column<?>[] { df1KeyColumn, df1ValueColumn },
				new String[] { "KEY", "VALUE" }, "KEY");

		NavigableMap<Integer, String> expected1 = new TreeMap<>(
				df1.stream().collect(Collectors.toMap(r -> r.get(0), r -> r.get(1))));
		NavigableMap<Integer, String> actual1 = df1.asMap("VALUE");
		Assertions.assertEquals(expected1, actual1);
		Assertions.assertEquals(expected1.descendingKeySet(), actual1.descendingKeySet());
		Assertions.assertEquals(expected1.descendingMap(), actual1.descendingMap());
		Assertions.assertEquals(expected1.descendingMap().subMap(4, 2), actual1.descendingMap().subMap(4, 2));
		Assertions.assertEquals(expected1.descendingMap().values().stream().collect(Collectors.toList()),
				actual1.descendingMap().values().stream().collect(Collectors.toList()));

		NavigableMap<Integer, Row> expected2 = new TreeMap<>(
				df1.stream().collect(Collectors.toMap(r -> r.get(0), r -> r)));
		NavigableMap<Integer, Row> actual2 = df1.asMap();
		Assertions.assertEquals(expected2, actual2);
		Assertions.assertEquals(expected2.descendingKeySet(), actual2.descendingKeySet());
		Assertions.assertEquals(expected2.descendingMap(), actual2.descendingMap());
		Assertions.assertEquals(expected2.descendingMap().subMap(4, 2), actual2.descendingMap().subMap(4, 2));
		Assertions.assertEquals(expected2.descendingMap().subMap(4, 2).toString(),
				actual2.descendingMap().subMap(4, 2).toString());
		Assertions.assertEquals(expected2.descendingMap().values().stream().collect(Collectors.toList()),
				actual2.descendingMap().values().stream().collect(Collectors.toList()));
	}

	@Test
	public void testIndexOrganize() {

		IntColumn df1KeyColumn = IntColumn.of(1, 2, 3, 4, 5).toDistinct();
		StringColumn df1ValueColumn = StringColumn.of("A", "B", "C", "D", "E").toDistinct();
		DataFrame df = DataFrameFactory.create(new Column<?>[] { df1KeyColumn, df1ValueColumn },
				new String[] { "KEY", "VALUE" }, "KEY");

		Assertions.assertSame(df, df.indexOrganize("KEY"));
		Assertions.assertTrue(df.equals(df.indexOrganize("VALUE"), true));

		IntColumn c1 = IntColumn.of(1, 2, 3, 4, 5);
		StringColumn c2 = StringColumn.of("E", "D", "C", "B", "A");
		DataFrame df2 = DataFrameFactory.create(new Column<?>[] { c1, c2 }, new String[] { "C1", "C2" });

		IntColumn c3 = IntColumn.of(5, 4, 3, 2, 1);
		StringColumn c4 = StringColumn.of("A", "B", "C", "D", "E").toDistinct();
		DataFrame expected = DataFrameFactory.create(new Column<?>[] { c3, c4 }, new String[] { "C1", "C2" }, "C2");

		Assertions.assertEquals(expected, df2.indexOrganize("C2"));

		IntColumn c5 = IntColumn.of(1, 1, 2, 2, 3, 3);
		StringColumn c6 = StringColumn.of("A", "B", "C", "D", "E", "F");
		DataFrame df3 = DataFrameFactory.create(new Column<?>[] { c5, c6 }, new String[] { "C1", "C2" });
		try {
			df3.indexOrganize(0);
			throw new RuntimeException("expecting Exception");
		} catch (Exception e) {
			// ok
		}
	}

	@Test
	public void testIndexOrganize2() {

		String[] names = { "A", "B", "C", "D", "E", "F", "G", "H", "I" };

		Column<?>[] unsorted = new Column<?>[9];
		{
			unsorted[0] = ByteColumn.of((byte) 4, (byte) 3, (byte) 1, (byte) 2, (byte) 5);
			unsorted[1] = ShortColumn.of((short) 4, (short) 3, (short) 1, (short) 2, (short) 5);
			unsorted[2] = IntColumn.of(4, 3, 1, 2, 5);
			unsorted[3] = FloatColumn.of(4f, 3f, 1f, 2f, 5f);
			unsorted[4] = DoubleColumn.of(4d, 3d, 1d, 2d, 5d);
			unsorted[5] = StringColumn.of("4", "3", "1", "2", "5");
			unsorted[6] = DecimalColumn.of(new BigDecimal("4"), new BigDecimal("3"), new BigDecimal("1"),
					new BigDecimal("2"), new BigDecimal("5"));

			int yyyymmd0 = 20190100;

			unsorted[7] = DateColumn.of(fromInt(yyyymmd0 + 4), fromInt(yyyymmd0 + 3), fromInt(yyyymmd0 + 1),
					fromInt(yyyymmd0 + 2), fromInt(yyyymmd0 + 5));

			unsorted[8] = DateTimeColumn.of(fromInt(yyyymmd0 + 4).atStartOfDay(), fromInt(yyyymmd0 + 3).atStartOfDay(),
					fromInt(yyyymmd0 + 1).atStartOfDay(), fromInt(yyyymmd0 + 2).atStartOfDay(),
					fromInt(yyyymmd0 + 5).atStartOfDay());
		}

		Column<?>[] sorted = new Column<?>[unsorted.length];
		for (int i = 0; i < sorted.length; i++)
			sorted[i] = unsorted[i].toDistinct();

		DataFrame expected = DataFrameFactory.create(sorted, names);

		for (int i = 0; i < unsorted.length; i++) {
			DataFrame df = DataFrameFactory.create(unsorted, names);
			Assertions.assertEquals(expected.withKeyColumn(i), df.indexOrganize(i));
		}
	}

	@Test
	public void testSort() {
		StringColumn a = StringColumn.of("D", "A", "D", "B", "B", "C", "A", "C", "D");
		IntColumn b = IntColumn.of(3, 2, 1, 1, 2, 2, 1, 1, 2);
		IntColumn c = IntColumn.of(9, 2, 7, 3, 4, 6, 1, 5, 8);

		DataFrame unsorted = DataFrameConfig.builder().columns(a, b, c).columnNames("C1", "C2", "C3").build().create();

		DataFrame expected = DataFrameConfig.builder()
				.columns(a.toSorted(), IntColumn.of(1, 2, 1, 2, 1, 2, 1, 2, 3), c.toSorted())
				.columnNames("C1", "C2", "C3").build().create();

		DataFrame actual = unsorted.sort("C1", "C2");

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testGroupBy() {

		StringColumn a = StringColumn.of("D", "A", "D", "B", "B", "C", "A", "C", "D");
		IntColumn b = IntColumn.of(3, 2, 1, 1, 2, 2, 1, 1, 2);
		IntColumn c = IntColumn.of(9, 2, 7, 3, 4, 6, 1, 5, 8);

		DataFrame data = DataFrameConfig.builder().columns(a, b, c).columnNames("C1", "C2", "C3").build().create();

		// C1, sum(C2) group by C1
		Assertions.assertEquals(
				DataFrameConfig.builder().columnNames("C1", "SUM")
						.columns(StringColumn.of("A", "B", "C", "D"), IntColumn.of(3, 3, 3, 6)).build().create(),
				data.groupBy(GroupByConfig.builder().groupByNames("C1").derivedNames("SUM").derivedTypes(ColumnType.INT)
						.reductions(s -> s.mapToInt(r -> r.getInt("C2")).sum()).build()));

		// C1 group by C1
		Assertions.assertEquals(DataFrameConfig.builder().columnNames("C1").columns(StringColumn.of("A", "B", "C", "D"))
				.build().create(), data.groupBy(GroupByConfig.builder().groupByNames("C1").build()));

		// C1, C2, C3 group by C1, C2, C3
		Assertions.assertEquals(data.sort("C1", "C2"),
				data.groupBy(GroupByConfig.builder().groupByNames("C1", "C2", "C3").build()));

		// C1, C2, max(C3) group by C1, C2
		Assertions.assertEquals(data.sort("C1", "C2"),
				data.groupBy(
						GroupByConfig.builder().groupByNames("C1", "C2").derivedNames("C3").derivedTypes(ColumnType.INT)
								.reductions(s -> s.mapToInt(r -> r.getInt("C3")).max().getAsInt()).build()));
	}

	@Test
	public void testAsResultSet() throws SQLException {

		StringColumn a = StringColumn.of("D", "A", null, null, "D", null, "B", "B", "C", "A", "C", "D");
		IntColumn b = IntColumn.of(3, 2, null, null, 1, null, 1, 2, 2, 1, 1, 2);
		DateTimeColumn c = b.toDateTimeColumn(i -> i == null ? null : LocalDateTime.now().plusDays(i));
		DecimalColumn d = b.toDecimalColumn(i -> i == null ? null : BigDecimal.valueOf(i));

		ResultSet rs = DataFrameConfig.builder().columns(a, b, c, d).columnNames("A", "B", "C", "D").build().create()
				.asResultSet();

		try {
			rs.getString(1);
			throw new RuntimeException("expected exception1");
		} catch (SQLException e) {
			/* good */
		}

		int i;
		for (i = 0; rs.next(); i++) {
			Assertions.assertEquals(a.get(i), rs.getString("A"));
			Assertions.assertEquals(a.isNull(i), rs.wasNull());

			Integer bx = b.get(i);
			Assertions.assertEquals(bx == null ? 0 : bx, rs.getInt("B"));
			Assertions.assertEquals(b.isNull(i), rs.wasNull());

			Timestamp ts = rs.getTimestamp("C");
			Assertions.assertEquals(c.get(i), ts == null ? null : ts.toLocalDateTime());
			Assertions.assertEquals(c.isNull(i), rs.wasNull());

			Assertions.assertEquals(d.get(i), rs.getBigDecimal("D"));
			Assertions.assertEquals(d.isNull(i), rs.wasNull());
		}

		try {
			rs.getString(1);
			throw new RuntimeException("expected exception2");
		} catch (SQLException e) {
			/* good */
		}

		Assertions.assertEquals(a.size(), i);
	}

	private static LocalDate fromInt(int yyyymmdd) {
		return LocalDate.of(yyyymmdd / 10000, yyyymmdd % 10000 / 100, yyyymmdd % 100);
	}

	DataFrame getDf(String label) {
		return DF_MAP.get(label);
	}
}
