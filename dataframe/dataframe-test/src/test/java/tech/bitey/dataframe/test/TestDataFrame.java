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

package tech.bitey.dataframe.test;

import static java.util.Spliterator.DISTINCT;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import tech.bitey.dataframe.ByteColumn;
import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.ColumnTypeCode;
import tech.bitey.dataframe.Cursor;
import tech.bitey.dataframe.DataFrame;
import tech.bitey.dataframe.DataFrameFactory;
import tech.bitey.dataframe.DateColumn;
import tech.bitey.dataframe.DateTimeColumn;
import tech.bitey.dataframe.DecimalColumn;
import tech.bitey.dataframe.DoubleColumn;
import tech.bitey.dataframe.FloatColumn;
import tech.bitey.dataframe.GroupByConfig;
import tech.bitey.dataframe.IntColumn;
import tech.bitey.dataframe.ReadCsvConfig;
import tech.bitey.dataframe.ReadFromDbConfig;
import tech.bitey.dataframe.Row;
import tech.bitey.dataframe.ShortColumn;
import tech.bitey.dataframe.StringColumn;
import tech.bitey.dataframe.WriteToDbConfig;
import tech.bitey.dataframe.db.BlobFromResultSet;
import tech.bitey.dataframe.db.BooleanFromResultSet;
import tech.bitey.dataframe.db.ByteFromResultSet;
import tech.bitey.dataframe.db.DateFromResultSet;
import tech.bitey.dataframe.db.DateTimeFromResultSet;
import tech.bitey.dataframe.db.DecimalFromResultSet;
import tech.bitey.dataframe.db.DoubleFromResultSet;
import tech.bitey.dataframe.db.FloatFromResultSet;
import tech.bitey.dataframe.db.IFromResultSet;
import tech.bitey.dataframe.db.InstantFromResultSet;
import tech.bitey.dataframe.db.IntFromResultSet;
import tech.bitey.dataframe.db.LongFromResultSet;
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
				new TestShortColumn(), new TestByteColumn(), new TestUuidColumn(), new TestNormalStringColumn(),
				new TestFixedAsciiColumn() };

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
				for (int i = 0; i < df.columnCount(); i++) {
					Object c = cursor.get(i);
					Object r = row.get(i);
					Assertions.assertEquals(c, r, test);
				}
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
	public void testJoinOneToMany() {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame df = e.getValue();

			if (!df.column(0).isDistinct())
				continue;

			DataFrame expected = df.withKeyColumn(0);
			expected = expected.join(expected);

			DataFrame actual = df.withKeyColumn(0);
			actual = actual.joinOneToMany(actual, actual.keyColumnName());

			Assertions.assertTrue(expected.equals(actual, true), e.getKey() + ", inner 1:1 vs inner 1:many");
		}
	}

	@Test
	public void testReadWriteBinary() throws Exception {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame expected = e.getValue();

			File file = File.createTempFile(e.getKey(), null);
			file.deleteOnExit();

			expected.writeTo(file);

			DataFrame copied = DataFrameFactory.readFrom(file);
			Assertions.assertEquals(expected, copied, e.getKey() + ", read/write binary (copied)");

			// first call to mapFrom may modify the file
			DataFrame mapped = DataFrameFactory.mapFrom(file);
			Assertions.assertEquals(expected, mapped, e.getKey() + ", read/write binary (mapped)");

			// second call to mapFrom should not modify file
			long lastModified = file.lastModified();
			DataFrame mapped2 = DataFrameFactory.mapFrom(file);
			Assertions.assertEquals(expected, mapped2, e.getKey() + ", read/write binary (mapped2)");
			Assertions.assertTrue(lastModified == file.lastModified(), e.getKey() + ", read/write binary (modified)");
		}
	}

	@Test
	public void testReadWriteCsv() throws Exception {

		for (Map.Entry<String, DataFrame> e : DF_MAP.entrySet()) {

			DataFrame expected = e.getValue();

			File file = File.createTempFile(e.getKey(), null);
			file.deleteOnExit();

			expected.writeCsvTo(file);
			DataFrame actual = DataFrameFactory.readCsvFrom(file, new ReadCsvConfig(expected.columnTypes()));

			Assertions.assertEquals(expected, actual, e.getKey() + ", read/write csv");
		}

		// test null vs empty string
		{
			StringColumn column = StringColumn.of(null, "", "foo", "", null);
			DataFrame expected = DataFrameFactory.create(new Column<?>[] { column }, new String[] { "COLUMN" });

			File file = File.createTempFile("emptyString", "csv");
			file.deleteOnExit();
			expected.writeCsvTo(file);

			DataFrame actual = DataFrameFactory.readCsvFrom(file, new ReadCsvConfig(ColumnType.STRING));

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
					case IN:
						create.append("TEXT");
						fromRsLogic.add(InstantFromResultSet.INSTANT_FROM_TIMESTAMP);
						break;
					case S:
					case NS:
					case FS:
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
					case BL:
						create.append("TEXT");
						fromRsLogic.add(BlobFromResultSet.INPUT_STREAM);
						break;
					}

					if (i < cc - 1)
						create.append(", ");
					else
						create.append(")");
				}

				stmt.execute(create.toString());

				StringBuilder insert = new StringBuilder();
				insert.append("insert into " + tableName + " values (?");
				for (int i = 0; i < cc - 1; i++)
					insert.append(",?");
				insert.append(")");
				try (PreparedStatement ps = conn.prepareStatement(insert.toString())) {

					expected.writeTo(ps, WriteToDbConfig.DEFAULT_CONFIG);

					ResultSet rs = stmt.executeQuery("select * from " + tableName);
					DataFrame actual = DataFrameFactory.readFrom(rs, new ReadFromDbConfig(fromRsLogic));
					Assertions.assertEquals(expected, actual, e.getKey() + ", read/write db");
				}
			}
		}
	}

	@Test
	public void testResultSetMetaData() throws Exception {

		String[] columnNames = new String[] { "A", "B", "C" };
		Column<?>[] columns = new Column<?>[] { IntColumn.of(1), StringColumn.of("ONE"),
				DecimalColumn.of(BigDecimal.ONE) };

		DataFrame df = DataFrameFactory.create(columns, columnNames);
		ResultSetMetaData rsmd = df.asResultSet().getMetaData();

		Assertions.assertEquals(df.columnCount(), rsmd.getColumnCount(), "mismatched column count");

		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			Assertions.assertEquals(df.columnName(i - 1), rsmd.getColumnName(i), "mismatched column name, i=" + i);
			Assertions.assertEquals(df.column(i - 1).getType().getElementType().getName(), rsmd.getColumnClassName(i),
					"mismatched column class name, i=" + i);
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
		Object from = keyColumn.first();
		Object to = keyColumn.last();

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

		DataFrame unsorted = DataFrameFactory.of("C1", a, "C2", b, "C3", c);

		DataFrame expected = DataFrameFactory.of("C1", a.toSorted(), "C2", IntColumn.of(1, 2, 1, 2, 1, 2, 1, 2, 3),
				"C3", c.toSorted());

		DataFrame actual = unsorted.sort("C1", "C2");

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testGroupBy() {

		StringColumn a = StringColumn.of("D", "A", "D", "B", "B", "C", "A", "C", "D");
		IntColumn b = IntColumn.of(3, 2, 1, 1, 2, 2, 1, 1, 2);
		IntColumn c = IntColumn.of(9, 2, 7, 3, 4, 6, 1, 5, 8);

		DataFrame data = DataFrameFactory.of("C1", a, "C2", b, "C3", c);

		// C1, sum(C2) group by C1
		Assertions.assertEquals(
				DataFrameFactory.of("C1", StringColumn.of("A", "B", "C", "D"), "SUM", IntColumn.of(3, 3, 3, 6)),
				data.groupBy(new GroupByConfig(List.of("C1"), List.of("SUM"), List.of(ColumnType.INT),
						List.of(s -> s.mapToInt(r -> r.getInt("C2")).sum()))));

		// C1 group by C1
		Assertions.assertEquals(DataFrameFactory.of("C1", StringColumn.of("A", "B", "C", "D")),
				data.groupBy(new GroupByConfig(List.of("C1"))));

		// C1, C2, C3 group by C1, C2, C3
		Assertions.assertEquals(data.sort("C1", "C2"), data.groupBy(new GroupByConfig(List.of("C1", "C2", "C3"))));

		// C1, C2, max(C3) group by C1, C2
		Assertions.assertEquals(data.sort("C1", "C2"),
				data.groupBy(new GroupByConfig(List.of("C1", "C2"), List.of("C3"), List.of(ColumnType.INT),
						List.of(s -> s.mapToInt(r -> r.getInt("C3")).max().getAsInt()))));
	}

	@Test
	public void testAsResultSet() throws SQLException {

		StringColumn a = StringColumn.of("D", "A", null, null, "D", null, "B", "B", "C", "A", "C", "D");
		IntColumn b = IntColumn.of(3, 2, null, null, 1, null, 1, 2, 2, 1, 1, 2);
		DateTimeColumn c = b.toDateTimeColumn(i -> i == null ? null : LocalDateTime.now().plusDays(i));
		DecimalColumn d = b.toDecimalColumn(i -> i == null ? null : BigDecimal.valueOf(i));

		ResultSet rs = DataFrameFactory.of("A", a, "B", b, "C", c, "D", d).asResultSet();

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

	@Test
	public void withDerivedColumn() {
		IntColumn a = IntColumn.of(1, 2, null, 3);
		StringColumn b = a.toStringColumn();

		DataFrame expected = DataFrameFactory.create(List.of(a, b), List.of("A", "B"));
		DataFrame actual = DataFrameFactory.create(List.of(a), List.of("A")).withDerivedColumn("B", ColumnType.STRING,
				r -> r.get("A") == null ? null : r.get("A").toString());
		Assertions.assertEquals(expected, actual);

		for (Row r : expected)
			for (int i = 0; i < r.columnCount(); i++)
				Assertions.assertEquals(expected.columnName(i), r.columnName(i));
	}

	@Test
	public void parseCsv() throws Exception {

		DataFrame expected = DataFrameFactory.of("A", StringColumn.of("a", "b", "c"), "B", IntColumn.of(1, 2, null));
		DataFrame actual;

		String vanilla = "A,B\na,\"1\"\nb,2\nc,";
		actual = DataFrameFactory.readCsvFrom(new ByteArrayInputStream(vanilla.getBytes()),
				new ReadCsvConfig(ColumnType.STRING, ColumnType.INT));
		Assertions.assertEquals(expected, actual);

		String fancy = "A\tB\r\na\t\"1\"\r\nb\t2\r\nc\t(null)";
		actual = DataFrameFactory.readCsvFrom(new ByteArrayInputStream(fancy.getBytes()),
				new ReadCsvConfig(ColumnType.STRING, ColumnType.INT).withDelim('\t').withNullValue("(null)"));
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testSelectColumn() throws Exception {

		IntColumn a = IntColumn.of(1, 2, 3).toDistinct();
		IntColumn b = IntColumn.of(11, 22, 33);
		IntColumn c = IntColumn.of(111, 222, 333);

		// include key column
		{
			DataFrame expected = DataFrameFactory.of("A", a, "B", b).withKeyColumn("A");

			DataFrame actual = DataFrameFactory.of("A", a, "B", b, "C", c).withKeyColumn("A").selectColumns("A", "B");

			Assertions.assertEquals(expected, actual);
		}

		// exclude key column
		{
			DataFrame expected = DataFrameFactory.of("B", b, "C", c);

			DataFrame actual = DataFrameFactory.of("A", a, "B", b, "C", c).withKeyColumn("A").selectColumns("B", "C");

			Assertions.assertEquals(expected, actual);
		}
	}

	@Test
	public void testRecords() {

		record TestRecord(boolean c0, int c1, Integer c2, long c3, Long c4, double c5, Double c6, float c7, Float c8,
				short c9, Short c10, byte c11, Byte c12, String c13, UUID c14, BigDecimal c15, LocalDate c16) {
		}

		List<TestRecord> expected = new ArrayList<>();

		expected.add(new TestRecord(false, 0, 0, 0L, 0L, 0d, 0d, 0f, 0f, (short) 0, (short) 0, (byte) 0, (byte) 0, "",
				new UUID(0, 0), BigDecimal.ZERO, LocalDate.EPOCH));
		expected.add(new TestRecord(false, 0, null, 0L, null, 0d, null, 0f, null, (short) 0, null, (byte) 0, null, null,
				null, null, null));
		expected.add(new TestRecord(true, 1, 1, 1L, 1L, 1d, 1d, 1f, 1f, (short) 1, (short) 1, (byte) 1, (byte) 1, "hi",
				UUID.randomUUID(), BigDecimal.ONE, LocalDate.now()));

		DataFrame df = DataFrameFactory.of(expected);

		List<TestRecord> actual = df.stream(TestRecord.class).toList();

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testRecordsEmpty() {

		record TestRecordEmpty(int foo, String bar) {
		}

		List<TestRecordEmpty> expected = List.of();

		DataFrame df = DataFrameFactory.of(TestRecordEmpty.class, expected);
		Assertions.assertEquals(df, DataFrameFactory.of("foo", IntColumn.of(), "bar", StringColumn.of()));

		List<TestRecordEmpty> actual = df.stream(TestRecordEmpty.class).toList();

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void filterToNull() {

		DataFrame df = DataFrameFactory
				.of("I", IntColumn.of(null, 1, null, 2, null), "S", StringColumn.of(null, "1", null, "2", null))
				.filter(r -> r.isNull(0));

		DataFrame actual = df.append(df).subFrame(0, 4);

		DataFrame expected = DataFrameFactory.of("I", IntColumn.builder().addNulls(4).build(), "S",
				StringColumn.builder().addNulls(4).build());

		Assertions.assertEquals(expected, actual);
	}

	private static LocalDate fromInt(int yyyymmdd) {
		return LocalDate.of(yyyymmdd / 10000, yyyymmdd % 10000 / 100, yyyymmdd % 100);
	}

	DataFrame getDf(String label) {
		return DF_MAP.get(label);
	}

	private Supplier<Object> sampleValue(ColumnTypeCode type) {
		return switch (type) {
		case B -> () -> true;
		case BD -> () -> BigDecimal.ONE;
		case BL -> () -> new ByteArrayInputStream("sample".getBytes());
		case D -> () -> (double) 100;
		case DA -> () -> LocalDate.of(2022, 12, 11);
		case DT -> () -> LocalDateTime.of(2022, 12, 11, 21, 30, 0);
		case F -> () -> (float) 100;
		case FS -> () -> "sample";
		case I -> () -> (int) 100;
		case IN -> () -> Instant.ofEpochSecond(10000000);
		case L -> () -> (long) 100;
		case NS -> () -> "sample";
		case S -> () -> "sample";
		case T -> () -> (short) 100;
		case TI -> () -> LocalTime.of(21, 30, 0);
		case UU -> () -> new UUID(123, 4560000);
		case Y -> () -> (byte) 100;
		default -> throw new IllegalArgumentException("Missing sample value for type: " + type);
		};
	}

	@Test
	public void getters() {

		LinkedHashMap<String, Column<?>> columnMap = new LinkedHashMap<>();
		for (ColumnTypeCode type : ColumnTypeCode.values())
			columnMap.put(type.name(), type.getType().builder().add(sampleValue(type).get()).build());

		DataFrame df = DataFrameFactory.create(columnMap);

		Set<ColumnTypeCode> types = EnumSet.noneOf(ColumnTypeCode.class);
		getters(df, types, ColumnTypeCode.B, df::booleanColumn, df::getBoolean, Row::getBoolean);
		getters(df, types, ColumnTypeCode.DA, df::dateColumn, df::getDate, Row::getDate);
		getters(df, types, ColumnTypeCode.DT, df::dateTimeColumn, df::getDateTime, Row::getDateTime);
		getters(df, types, ColumnTypeCode.TI, df::timeColumn, df::getTime, Row::getTime);
		getters(df, types, ColumnTypeCode.IN, df::instantColumn, df::getInstant, Row::getInstant);
		getters(df, types, ColumnTypeCode.D, df::doubleColumn, df::getDouble, Row::getDouble);
		getters(df, types, ColumnTypeCode.F, df::floatColumn, df::getFloat, Row::getFloat);
		getters(df, types, ColumnTypeCode.I, df::intColumn, df::getInt, Row::getInt);
		getters(df, types, ColumnTypeCode.L, df::longColumn, df::getLong, Row::getLong);
		getters(df, types, ColumnTypeCode.T, df::shortColumn, df::getShort, Row::getShort);
		getters(df, types, ColumnTypeCode.Y, df::byteColumn, df::getByte, Row::getByte);
		getters(df, types, ColumnTypeCode.S, df::stringColumn, df::getString, Row::getString);
		getters(df, types, ColumnTypeCode.NS, df::stringColumn, df::getString, Row::getString);
		getters(df, types, ColumnTypeCode.FS, df::stringColumn, df::getString, Row::getString);
		getters(df, types, ColumnTypeCode.BD, df::decimalColumn, df::getBigDecimal, Row::getBigDecimal);
		getters(df, types, ColumnTypeCode.UU, df::uuidColumn, df::getUuid, Row::getUuid);
		getters(df, types, ColumnTypeCode.BL, df::blobColumn, df::getBlob, Row::getBlob, (a, b) -> {
			try {
				return Arrays.equals(a.readAllBytes(), b.readAllBytes());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		if (types.size() < columnMap.size())
			throw new RuntimeException(
					"Missing tests for: " + Sets.difference(EnumSet.allOf(ColumnTypeCode.class), types));
	}

	private <E> void getters(DataFrame df, Set<ColumnTypeCode> types, ColumnTypeCode type,
			Function<String, Column<E>> column, BiFunction<Integer, String, E> getValue,
			BiFunction<Row, String, E> row) {
		getters(df, types, type, column, getValue, row, Object::equals);
	}

	@SuppressWarnings("unchecked")
	private <E> void getters(DataFrame df, Set<ColumnTypeCode> types, ColumnTypeCode type,
			Function<String, Column<E>> column, BiFunction<Integer, String, E> getValue, BiFunction<Row, String, E> row,
			BiPredicate<E, E> comparator) {

		E columnValue = column.apply(type.name()).get(0);
		E dfValue = getValue.apply(0, type.name());
		E rowValue = row.apply(df.get(0), type.name());

		assertTrue(comparator.test((E) sampleValue(type).get(), columnValue), type + " column value");
		assertTrue(comparator.test((E) sampleValue(type).get(), dfValue), type + " df value");
		assertTrue(comparator.test((E) sampleValue(type).get(), rowValue), type + " row value");

		types.add(type);
	}
}
