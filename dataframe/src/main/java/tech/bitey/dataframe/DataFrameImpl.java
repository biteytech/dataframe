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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static tech.bitey.dataframe.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.DfPreconditions.checkElementIndex;
import static tech.bitey.dataframe.DfPreconditions.checkNotNull;
import static tech.bitey.dataframe.DfPreconditions.checkPositionIndex;
import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import tech.bitey.bufferstuff.BufferBitSet;

@SuppressWarnings({ "rawtypes", "unchecked" })
class DataFrameImpl extends AbstractList<Row> implements DataFrame {

	private static final DataFramePrinter DEFAULT_PRINTER = new DataFramePrinter(20);

	/*--------------------------------------------------------------------------------
	 *	Immutable State
	 *--------------------------------------------------------------------------------*/
	final Integer keyIndex;

	final String[] columnNames;
	final Column<?>[] columns;
	final Map<String, Integer> columnToIndexMap;

	/*--------------------------------------------------------------------------------
	 *	Constructors
	 *--------------------------------------------------------------------------------*/
	private DataFrameImpl(Column<?>[] columns, String[] columnNames, Integer keyIndex,
			Map<String, Integer> columnToIndexMap) {

		this.columns = columns;
		this.columnNames = columnNames;
		this.keyIndex = keyIndex;
		this.columnToIndexMap = columnToIndexMap;
	}

	private DataFrameImpl(Column<?>[] columns, String[] columnNames, Integer keyIndex) {

		this.columns = columns;
		this.columnNames = columnNames;
		this.keyIndex = keyIndex;

		columnToIndexMap = new HashMap<>();
		for (int i = 0; i < columnNames.length; i++)
			columnToIndexMap.put(columnNames[i], i);
	}

	DataFrameImpl(LinkedHashMap<String, Column<?>> columnMap, String keyColumnName) {

		checkNotNull(columnMap, "columnMap cannot be null");
		checkArgument(!columnMap.isEmpty(), "columnMap cannot be empty");
		checkArgument(!columnMap.containsKey(null), "column name cannot be null");
		checkArgument(!columnMap.containsValue(null), "column cannot be null");

		if (keyColumnName != null) {
			checkArgument(columnMap.containsKey(keyColumnName), "no such column name: " + keyColumnName);
			checkArgument(columnMap.get(keyColumnName).isDistinct(), "key column must be a unique index");
		}

		this.columns = new Column[columnMap.size()];
		this.columnNames = new String[columnMap.size()];

		columnToIndexMap = new HashMap<>();

		int i = 0;
		for (Map.Entry<String, Column<?>> e : columnMap.entrySet()) {
			columns[i] = e.getValue();
			columnNames[i] = e.getKey();
			columnToIndexMap.put(e.getKey(), i);
			i++;
		}

		if (keyColumnName != null)
			keyIndex = columnToIndexMap.get(keyColumnName);
		else
			keyIndex = null;

		// validate that all columns have the same size
		int size = columns[0].size();
		for (i = 1; i < columns.length; i++)
			checkArgument(columns[i].size() == size, "all columns must have the same size");
	}

	/*--------------------------------------------------------------------------------
	 *	Precondition Utility Methods
	 *--------------------------------------------------------------------------------*/
	private int checkedColumnIndex(String columnName) {
		checkArgument(columnToIndexMap.containsKey(columnName), "no such column name: " + columnName);
		return columnToIndexMap.get(columnName);
	}

	private String checkedColumnName(int columnIndex) {
		checkElementIndex(columnIndex, columnNames.length);
		return columnNames[columnIndex];
	}

	private Column<?> checkedColumn(int columnIndex) {
		checkElementIndex(columnIndex, columns.length);
		return columns[columnIndex];
	}

	private Column<?> checkedColumn(String columnName) {
		int columnIndex = checkedColumnIndex(columnName);
		return columns[columnIndex];
	}

	private NonNullColumn checkedKeyColumn(String operation) {
		if (!hasKeyColumn())
			throw new UnsupportedOperationException(operation + ", missing key column");

		NonNullColumn keyColumn = (NonNullColumn) columns[keyIndex];
		return keyColumn;
	}

	private String[] checkedIndicesToNames(int[] columnIndices) {

		String[] columnNames = new String[columnIndices.length];
		for (int i = 0; i < columnNames.length; i++)
			columnNames[i] = checkedColumnName(columnIndices[i]);

		return columnNames;
	}

	/*--------------------------------------------------------------------------------
	 *	Object, Collection, and List Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public String toString() {
		return DEFAULT_PRINTER.print(this);
	}

	@Override
	public int size() {
		return columns[0].size();
	}

	@Override
	public Row get(int rowIndex) {
		checkElementIndex(rowIndex, size());
		return new RowImpl(rowIndex);
	}

	@Override
	public boolean equals(DataFrame df, boolean dataOnly) {

		DataFrameImpl rhs = (DataFrameImpl) df;

		if (!Arrays.equals(columns, rhs.columns))
			return false;

		return dataOnly || (Arrays.equals(columnNames, rhs.columnNames) && Objects.equals(keyIndex, rhs.keyIndex));
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof DataFrameImpl))
			return false;
		return equals((DataFrame) o, false);
	}

	@Override
	public DataFrame copy() {
		return modifyColumns(Column::copy);
	}

	/*--------------------------------------------------------------------------------
	 *	Miscellaneous Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public Cursor cursor(int index) {
		checkPositionIndex(index, size());
		return new CursorImpl(index);
	}

	@Override
	public <K, V> Map<K, V> toMap(int columnIndex) {
		return toMap(checkedColumn(columnIndex));
	}

	@Override
	public <K, V> Map<K, V> toMap(String columnName) {
		return toMap(checkedColumn(columnName));
	}

	private <K, V> Map<K, V> toMap(Column<?> valueColumn) {

		Column<?> keyColumn = checkedKeyColumn("toMap");

		Map map = new ColumnBackedMap<>(keyColumn, valueColumn);
		return map;
	}

	@Override
	public DateSeries toDateSeries(int columnIndex) {
		return toDateSeries(checkedColumn(columnIndex), columnNames[columnIndex]);
	}

	@Override
	public DateSeries toDateSeries(String columnName) {
		return toDateSeries(checkedColumn(columnName), columnName);
	}

	private DateSeries toDateSeries(Column<?> values, String valueColumnName) {
		checkState(hasKeyColumn(), "dataframe must have a key column");
		checkState(keyColumnType() == ColumnType.DATE, "key column must have type DATE");

		checkState(values.getType() == ColumnType.DOUBLE, "values column must have type DOUBLE");
		checkState(values.isNonnull(), "values column must be non-null");

		return new DateSeriesImpl((NonNullDateColumn) columns[keyIndex], columnNames[keyIndex],
				(NonNullDoubleColumn) values, valueColumnName);
	}

	/*--------------------------------------------------------------------------------
	 *	Key Column Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public boolean hasKeyColumn() {
		return keyIndex != null;
	}

	@Override
	public Integer keyColumnIndex() {
		return keyIndex;
	}

	@Override
	public String keyColumnName() {
		return keyIndex == null ? null : columnNames[keyIndex];
	}

	@Override
	public ColumnType keyColumnType() {
		return keyIndex == null ? null : columns[keyIndex].getType();
	}

	@Override
	public DataFrame withKeyColumn(int columnIndex) {

		String columnName = checkedColumnName(columnIndex);
		return withKeyColumn(columnName);
	}

	@Override
	public DataFrame withKeyColumn(String columnName) {

		int keyIndex = checkedColumnIndex(columnName);

		if (hasKeyColumn() && keyIndex == this.keyIndex)
			return this;

		checkArgument(columns[keyIndex].isDistinct(),
				"column must be a unique index (isDistinct) to act as a key column");

		return new DataFrameImpl(columns, columnNames, keyIndex, columnToIndexMap);
	}

	@Override
	public DataFrame indexOrganize(int columnIndex) {

		Column column = checkedColumn(columnIndex);

		if (column.isSorted())
			return withKeyColumn(columnIndex);

		checkArgument(column.isNonnull(), "cannot create key column from nullable column");

		Object[] pair = ((NonNullColumn) column).toDistinctWithIndices();
		Column distinct = (Column) pair[0];
		IntColumn indices = (IntColumn) pair[1];

		Column[] columns = new Column[this.columns.length];
		for (int i = 0; i < columns.length; i++) {
			if (i == columnIndex)
				columns[i] = distinct;
			else
				columns[i] = ((AbstractColumn) this.columns[i]).select(indices);
		}

		return new DataFrameImpl(columns, columnNames, columnIndex, columnToIndexMap);
	}

	@Override
	public DataFrame indexOrganize(String columnName) {

		int columnIndex = checkedColumnIndex(columnName);
		return indexOrganize(columnIndex);
	}

	/*--------------------------------------------------------------------------------
	 *	Column Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public int columnCount() {
		return columns.length;
	}

	@Override
	public int columnIndex(String columnName) {
		return checkedColumnIndex(columnName);
	}

	@Override
	public String columnName(int columnIndex) {
		return checkedColumnName(columnIndex);
	}

	@Override
	public ColumnType columnType(int columnIndex) {
		return checkedColumn(columnIndex).getType();
	}

	@Override
	public ColumnType columnType(String columnName) {
		return checkedColumn(columnName).getType();
	}

	@Override
	public LinkedHashMap<String, Column<?>> columnMap() {

		LinkedHashMap<String, Column<?>> map = new LinkedHashMap<>();

		for (int i = 0; i < columns.length; i++)
			map.put(columnNames[i], columns[i]);

		return map;
	}

	@Override
	public List<Column<?>> columns() {
		return new ArrayList<>(Arrays.asList(columns));
	}

	@Override
	public List<String> columnNames() {
		return new ArrayList<>(Arrays.asList(columnNames));
	}

	@Override
	public List<ColumnType<?>> columnTypes() {
		return columns().stream().map(Column::getType).collect(Collectors.toList());
	}

	@Override
	public DataFrame withColumn(String columnName, Column<?> column) {
		return withColumns(new String[] { columnName }, new Column<?>[] { column });
	}

	@Override
	public DataFrame withColumns(String[] columnNames, Column<?>[] columns) {
		checkArgument(columnNames.length == columns.length, "column arrays must have same length");

		LinkedHashMap<String, Column<?>> columnMap = columnMap();

		for (int i = 0; i < columnNames.length; i++)
			columnMap.put(columnNames[i], columns[i]);

		return new DataFrameImpl(columnMap, keyColumnName());
	}

	@Override
	public DataFrame withColumns(LinkedHashMap<String, Column<?>> columns) {

		LinkedHashMap<String, Column<?>> columnMap = columnMap();
		columnMap.putAll(columns);

		return new DataFrameImpl(columnMap, keyColumnName());
	}

	@Override
	public DataFrame withColumns(DataFrame df) {
		return withColumns(df.columnMap());
	}

	@Override
	public DataFrame selectColumns(List<String> columnNames) {

		LinkedHashMap<String, Column<?>> columnMap = new LinkedHashMap<>();

		for (String columnName : columnNames) {
			Column<?> column = checkedColumn(columnName);
			columnMap.put(columnName, column);
		}

		return new DataFrameImpl(columnMap, keyColumnName());
	}

	@Override
	public DataFrame selectColumns(String... columnNames) {
		return selectColumns(Arrays.asList(columnNames));
	}

	@Override
	public DataFrame selectColumns(int... columnIndices) {
		return selectColumns(checkedIndicesToNames(columnIndices));
	}

	@Override
	public DataFrame dropColumns(Collection<String> columnNames) {
		// check that all column names are valid
		for (String columnName : columnNames)
			checkedColumn(columnName);

		Set<String> drop = new HashSet<>(columnNames);

		LinkedHashMap<String, Column<?>> columnMap = columnMap();
		for (String columnName : columnNames)
			if (drop.contains(columnName))
				columnMap.remove(columnName);

		String keyColumnName = keyColumnName();
		if (drop.contains(keyColumnName))
			keyColumnName = null;

		return new DataFrameImpl(columnMap, keyColumnName);
	}

	@Override
	public DataFrame dropColumns(String... columnNames) {
		return dropColumns(Arrays.asList(columnNames));
	}

	@Override
	public DataFrame dropColumns(int... columnIndices) {
		return dropColumns(checkedIndicesToNames(columnIndices));
	}

	@Override
	public <T> Column<T> column(int columnIndex) {
		return (Column<T>) checkedColumn(columnIndex);
	}

	@Override
	public StringColumn stringColumn(int columnIndex) {
		return (StringColumn) checkedColumn(columnIndex);
	}

	@Override
	public DecimalColumn decimalColumn(int columnIndex) {
		return (DecimalColumn) checkedColumn(columnIndex);
	}

	@Override
	public BooleanColumn booleanColumn(int columnIndex) {
		return (BooleanColumn) checkedColumn(columnIndex);
	}

	@Override
	public IntColumn intColumn(int columnIndex) {
		return (IntColumn) checkedColumn(columnIndex);
	}

	@Override
	public LongColumn longColumn(int columnIndex) {
		return (LongColumn) checkedColumn(columnIndex);
	}

	@Override
	public ShortColumn shortColumn(int columnIndex) {
		return (ShortColumn) checkedColumn(columnIndex);
	}

	@Override
	public DoubleColumn doubleColumn(int columnIndex) {
		return (DoubleColumn) checkedColumn(columnIndex);
	}

	@Override
	public FloatColumn floatColumn(int columnIndex) {
		return (FloatColumn) checkedColumn(columnIndex);
	}

	@Override
	public DateColumn dateColumn(int columnIndex) {
		return (DateColumn) checkedColumn(columnIndex);
	}

	@Override
	public DateTimeColumn dateTimeColumn(int columnIndex) {
		return (DateTimeColumn) checkedColumn(columnIndex);
	}

	@Override
	public <T> Column<T> column(String columnName) {
		return (Column<T>) checkedColumn(columnName);
	}

	@Override
	public StringColumn stringColumn(String columnName) {
		return (StringColumn) checkedColumn(columnName);
	}

	@Override
	public DecimalColumn decimalColumn(String columnName) {
		return (DecimalColumn) checkedColumn(columnName);
	}

	@Override
	public BooleanColumn booleanColumn(String columnName) {
		return (BooleanColumn) checkedColumn(columnName);
	}

	@Override
	public IntColumn intColumn(String columnName) {
		return (IntColumn) checkedColumn(columnName);
	}

	@Override
	public LongColumn longColumn(String columnName) {
		return (LongColumn) checkedColumn(columnName);
	}

	@Override
	public ShortColumn shortColumn(String columnName) {
		return (ShortColumn) checkedColumn(columnName);
	}

	@Override
	public DoubleColumn doubleColumn(String columnName) {
		return (DoubleColumn) checkedColumn(columnName);
	}

	@Override
	public FloatColumn floatColumn(String columnName) {
		return (FloatColumn) checkedColumn(columnName);
	}

	@Override
	public DateColumn dateColumn(String columnName) {
		return (DateColumn) checkedColumn(columnName);
	}

	@Override
	public DateTimeColumn dateTimeColumn(String columnName) {
		return (DateTimeColumn) checkedColumn(columnName);
	}

	@Override
	public <T> Column<T> deriveColumn(ColumnType<T> type, Function<Row, T> function) {

		ColumnBuilder<T> builder = type.builder();
		builder.ensureCapacity(size());

		for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.apply(cursor));

		return builder.build();
	}

	@Override
	public IntColumn deriveColumn(ToIntFunction<Row> function) {

		IntColumnBuilder builder = IntColumn.builder();
		builder.ensureCapacity(size());

		for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsInt(cursor));

		return builder.build();
	}

	@Override
	public LongColumn deriveColumn(ToLongFunction<Row> function) {

		LongColumnBuilder builder = LongColumn.builder();
		builder.ensureCapacity(size());

		for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsLong(cursor));

		return builder.build();
	}

	@Override
	public DoubleColumn deriveColumn(ToDoubleFunction<Row> function) {

		DoubleColumnBuilder builder = DoubleColumn.builder();
		builder.ensureCapacity(size());

		for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsDouble(cursor));

		return builder.build();
	}

	@Override
	public FloatColumn deriveColumn(ToFloatFunction<Row> function) {

		FloatColumnBuilder builder = FloatColumn.builder();
		builder.ensureCapacity(size());

		for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsFloat(cursor));

		return builder.build();
	}

	@Override
	public BooleanColumn deriveColumn(Predicate<Row> function) {

		BooleanColumnBuilder builder = BooleanColumn.builder();
		builder.ensureCapacity(size());

		for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.test(cursor));

		return builder.build();
	}

	/*--------------------------------------------------------------------------------
	 *	Row Selection Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public DataFrame sampleN(int sampleSize) {
		checkPositionIndex(sampleSize, size());

		if (isEmpty())
			return this;
		else if (sampleSize == 0)
			return empty();
		else if (sampleSize == size())
			return this;

		return filter(BufferBitSet.random(sampleSize, size()));
	}

	@Override
	public DataFrame sampleX(double proportion) {
		checkArgument(proportion >= 0d && proportion <= 1d, "proportion must be between 0 and 1 inclusive");
		return sampleN((int) Math.round(size() * proportion));
	}

	@Override
	public DataFrame head(int count) {
		count = Math.min(size(), count);
		return subFrame(0, count);
	}

	@Override
	public DataFrame tail(int count) {
		count = Math.min(size(), count);
		return subFrame(size() - count, size());
	}

	@Override
	public DataFrame subFrame(int fromIndex, int toIndex) {
		return modifyColumns(column -> column.subColumn(fromIndex, toIndex));
	}

	private int indexOrInsertionPoint(NonNullColumn keyColumn, Object o) {

		int index = keyColumn.findIndexOrInsertionPoint(o);

		if (index < 0) {
			// index is (-(insertion point) - 1)
			index = -(index + 1);
		}

		return index;
	}

	@Override
	public DataFrame headTo(Object toKey) {
		NonNullColumn keyColumn = checkedKeyColumn("headTo");

		int toIndex = indexOrInsertionPoint(keyColumn, toKey);

		return subFrame(0, toIndex);
	}

	@Override
	public DataFrame tailFrom(Object fromKey) {
		NonNullColumn keyColumn = checkedKeyColumn("tailFrom");

		int fromIndex = indexOrInsertionPoint(keyColumn, fromKey);

		return subFrame(fromIndex, size());
	}

	@Override
	public DataFrame subFrameByValue(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive) {
		NonNullColumn keyColumn = checkedKeyColumn("subFrameByValue");

		NonNullColumn subColumn = keyColumn.subColumnByValue(fromKey, fromInclusive, toKey, toInclusive);

		if (subColumn.isEmpty())
			return empty();

		int offset = subColumn.offset - keyColumn.offset;
		return subFrame(offset, offset + subColumn.size);
	}

	@Override
	public DataFrame filter(Predicate<Row> criteria) {

		BufferBitSet keep = new BufferBitSet();

		int i = 0;
		for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next(), i++)
			if (criteria.test(cursor))
				keep.set(i);

		return filter(keep);
	}

	@Override
	public DataFrame filterNulls() {

		BufferBitSet keep = null;

		for (int i = 0; i < columns.length; i++) {
			if (!columns[i].isNonnull()) {
				NullableColumn n = (NullableColumn) columns[i];
				BufferBitSet nonNulls = n.nonNulls.get(n.offset, n.offset + n.size);
				if (keep == null)
					keep = nonNulls;
				else
					keep.and(nonNulls);
			}
		}

		return keep == null ? this : filter(keep);
	}

	/*--------------------------------------------------------------------------------
	 *	Database-like Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public DataFrame append(DataFrame df) {
		return append(df, false);
	}

	@Override
	public DataFrame append(DataFrame df, boolean coerce) {

		checkArgument(columnCount() == df.columnCount(), "mismatched column counts");

		Column<?>[] columns = new Column[columnCount()];
		for (int i = 0; i < columnCount(); i++) {
			checkArgument(columnType(i) == df.columnType(i), "mismatched column types");

			columns[i] = this.columns[i].append(df.column(i), coerce);
		}

		Integer keyIndex = this.keyIndex;
		if (coerce && keyIndex != null && !columns[keyIndex].isDistinct())
			keyIndex = null;

		return new DataFrameImpl(columns, columnNames, keyIndex, columnToIndexMap);
	}

	@Override
	public DataFrame join(DataFrame df) {
		checkArgument(hasKeyColumn() && df.hasKeyColumn(), "both dataframes must have a key column");
		checkArgument(columnType(keyColumnIndex()) == df.columnType(df.keyColumnIndex()),
				"key columns must be of the same type");

		DataFrameImpl rhs = (DataFrameImpl) df;
		AbstractColumn leftKey = (AbstractColumn) columns[keyIndex];
		AbstractColumn rightKey = (AbstractColumn) rhs.columns[rhs.keyIndex];

		BufferBitSet keepLeft = new BufferBitSet();
		BufferBitSet keepRight = new BufferBitSet();

		int cardinality = leftKey.intersectBothSorted(rightKey, keepLeft, keepRight);

		String[] columnNames = jointColumnNames(rhs, rhs.keyIndex);

		Column<?>[] columns = new Column<?>[columnCount() + rhs.columnCount() - 1];
		for (int i = 0; i < columnCount(); i++)
			columns[i] = ((AbstractColumn) this.columns[i]).applyFilter(keepLeft, cardinality);
		for (int i = 0, j = 0; i < rhs.columnCount(); i++) {
			if (i != rhs.keyIndex)
				columns[j++ + columnCount()] = ((AbstractColumn) rhs.columns[i]).applyFilter(keepRight, cardinality);
		}

		return new DataFrameImpl(columns, columnNames, keyIndex);
	}

	private static String nextColumnName(Set<String> names, String name) {
		if (!names.contains(name))
			return name;

		for (int i = 2;; i++) {
			String nameWithSuffix = name + "_" + i;
			if (!names.contains(nameWithSuffix))
				return nameWithSuffix;
		}
	}

	private String[] jointColumnNames(DataFrame rhs, int rightColumnIndex) {
		return jointColumnNames(rhs, new int[] { rightColumnIndex });
	}

	private String[] jointColumnNames(DataFrame rhs, int[] rightColumnIndices) {

		Set<String> names = new HashSet<>(Arrays.asList(this.columnNames));

		String[] columnNames = Arrays.copyOf(this.columnNames,
				columnCount() + rhs.columnCount() - rightColumnIndices.length);

		for (int i = 0, j = columnCount(), k = 0; i < rhs.columnCount(); i++) {
			if (i != rightColumnIndices[k])
				columnNames[j++] = nextColumnName(names, rhs.columnName(i));
			else {
				k++;
				if (k == rightColumnIndices.length)
					k = 0;
			}
		}

		return columnNames;
	}

	@Override
	public DataFrame joinOneToMany(DataFrame df, String columnName) {
		return joinSingleIndex(df, columnName).df;
	}

	@Override
	public DataFrame joinManyToOne(DataFrame df, String columnName) {

		DataFrameImpl backasswards = ((DataFrameImpl) df).joinSingleIndex(this, columnName).df;

		Column<?>[] columns = new Column<?>[backasswards.columnCount()];
		System.arraycopy(backasswards.columns, df.columnCount(), columns, 0, this.columnCount() - 1);
		System.arraycopy(backasswards.columns, 0, columns, this.columnCount() - 1, df.columnCount());

		int idx = this.columnCount() - 1 + df.keyColumnIndex();
		Column<?> indexColumn = columns[idx];

		List<Column<?>> columnList = new ArrayList<>(Arrays.asList(columns));
		columnList.remove(idx);
		columnList.add(columnIndex(columnName), indexColumn);

		String[] columnNames = jointColumnNames(df, df.keyColumnIndex());

		return new DataFrameImpl(columnList.toArray(new Column<?>[0]), columnNames, null);
	}

	private static class JoinSingleIndexResult {
		final DataFrameImpl df;
		final IntColumn indices;

		JoinSingleIndexResult(DataFrameImpl data, IntColumn indices) {
			this.df = data;
			this.indices = indices;
		}
	}

	private JoinSingleIndexResult joinSingleIndex(DataFrame df, String columnName) {

		checkArgument(hasKeyColumn(), "missing key column");

		DataFrameImpl rhs = (DataFrameImpl) df;
		AbstractColumn leftKey = (AbstractColumn) columns[keyIndex];
		AbstractColumn rightColumn = (AbstractColumn) rhs.column(columnName);
		int rightColumnIndex = rhs.columnToIndexMap.get(columnName);

		checkArgument(leftKey.getType() == rightColumn.getType(), "columns being joined on must have the same type");

		BufferBitSet keepRight = new BufferBitSet();
		IntColumn indices = leftKey.intersectLeftSorted(rightColumn, keepRight);

		rhs = rhs.filter(keepRight);

		String[] columnNames = jointColumnNames(rhs, rightColumnIndex);

		Column<?>[] columns = new Column<?>[columnCount() + rhs.columnCount() - 1];
		for (int i = 0; i < columnCount(); i++)
			columns[i] = ((AbstractColumn) this.columns[i]).select(indices);
		for (int i = 0, j = 0; i < rhs.columnCount(); i++) {
			if (i != rightColumnIndex)
				columns[j++ + columnCount()] = rhs.columns[i];
		}

		DataFrameImpl result = new DataFrameImpl(columns, columnNames, null);
		return new JoinSingleIndexResult(result, indices);
	}

	@Override
	public DataFrame joinLeftOneToMany(DataFrame df, String rightColumnName) {

		JoinSingleIndexResult pair = joinSingleIndex(df, rightColumnName);
		DataFrameImpl inner = pair.df;
		IntColumn indices = pair.indices;

		BufferBitSet unmatchedLeft = new BufferBitSet();
		unmatchedLeft.set(0, this.size());
		for (int i = 0; i < indices.size(); i++)
			unmatchedLeft.set(indices.getInt(i), false);

		if (unmatchedLeft.cardinality() == 0)
			return inner;

		DataFrameImpl left = this.filter(unmatchedLeft);
		DataFrameImpl rhs = (DataFrameImpl) df;

		Column<?>[] columns = new Column<?>[columnCount() + df.columnCount() - 1];
		for (int i = 0; i < columnCount(); i++)
			columns[i] = left.columns[i];
		int rightColumnIndex = rhs.columnToIndexMap.get(rightColumnName);
		for (int i = 0, j = 0; i < rhs.columnCount(); i++) {
			if (i != rightColumnIndex)
				columns[j++ + columnCount()] = rhs.columns[i].getType().nullColumn(left.size());
		}

		left = new DataFrameImpl(columns, inner.columnNames, null);

		return inner.append(left, true);
	}

	@Override
	public DataFrame join(DataFrame df, String[] leftColumnNames, String[] rightColumnNames) {

		DataFrameImpl rhs = (DataFrameImpl) df;

		checkArgument(leftColumnNames.length == rightColumnNames.length,
				"left and right column name arrays must have the same length");
		checkArgument(leftColumnNames.length > 0, "column name arrays cannot be empty");

		int[] leftColumnIndices = new int[leftColumnNames.length];
		for (int i = 0; i < leftColumnNames.length; i++)
			leftColumnIndices[i] = checkedColumnIndex(leftColumnNames[i]);

		int[] rightColumnIndices = new int[rightColumnNames.length];
		for (int i = 0; i < rightColumnNames.length; i++)
			rightColumnIndices[i] = checkedColumnIndex(rightColumnNames[i]);

		for (int i = 0; i < leftColumnIndices.length; i++)
			checkArgument(columns[leftColumnIndices[i]].getType() == rhs.columns[rightColumnIndices[i]].getType(),
					"mismatched key column types");

		IntColumn indices;
		BufferBitSet keepRight = new BufferBitSet();

		{
			Map<Row, Integer> hashMap = new HashMap<>();

			for (Row row : selectColumns(leftColumnIndices)) {
				if (hashMap.containsKey(row))
					throw new IllegalStateException("columns do not form a unqiue index");
				else
					hashMap.put(row, row.rowIndex());
			}

			IntColumnBuilder builder = IntColumn.builder();

			for (Row row : rhs.selectColumns(rightColumnIndices)) {
				int leftRowIndex = hashMap.getOrDefault(row, -1);
				if (leftRowIndex >= 0) {
					builder.add(leftRowIndex);
					keepRight.set(row.rowIndex());
				}
			}

			indices = builder.build();
		}

		DataFrameImpl left = select(indices);
		DataFrameImpl right = rhs.filter(keepRight);

		String[] columnNames = jointColumnNames(right, rightColumnIndices);

		Column<?>[] columns = Arrays.copyOf(left.columns, columnNames.length);
		for (int i = 0, j = columnCount(), k = 0; i < right.columnCount(); i++) {
			if (i != rightColumnIndices[k])
				columns[j++] = right.columns[i];
			else {
				k++;
				if (k == rightColumnIndices.length)
					k = 0;
			}
		}

		return new DataFrameImpl(columns, columnNames, null);
	}

	/*--------------------------------------------------------------------------------
	 *	Export Methods
	 *--------------------------------------------------------------------------------*/

	@Override
	public void writeTo(File file) throws IOException {
		try (FileChannel fileChannel = FileChannel.open(file.toPath(), WRITE, CREATE, TRUNCATE_EXISTING);) {
			writeTo(fileChannel);
		}
	}

	@Override
	public void writeTo(WritableByteChannel channel) throws IOException {
		FileDataFrameHeader dfHeader = new FileDataFrameHeader(this);
		dfHeader.writeTo(channel);

		for (int i = 0; i < columnCount(); i++) {
			FileColumnHeader columnHeader = new FileColumnHeader(this, i);
			columnHeader.writeTo(channel);
		}

		for (Column<?> column : columns)
			((AbstractColumn) column).writeTo(channel);
	}

	@Override
	public void writeCsvTo(File file) throws IOException {
		try (FileOutputStream fos = new FileOutputStream(file);) {
			writeCsvTo(fos);
		}
	}

	private static final byte[] EMPTY_STRING = "\"\"".getBytes(UTF_8);

	@Override
	public void writeCsvTo(OutputStream os) throws IOException {

		Matcher escapeRequired = Pattern.compile("\"|,|\\R").matcher("");

		try (BufferedOutputStream bos = new BufferedOutputStream(os);) {
			// write header
			for (int i = 0; i < columnNames.length; i++) {
				bos.write(csvEscape(columnNames[i], escapeRequired).getBytes(UTF_8));
				terminateCsvField(bos, i == columns.length - 1);
			}

			// write body
			for (Cursor cursor = cursor(); cursor.hasNext(); cursor.next()) {
				for (int i = 0; i < columns.length; i++) {
					if (!cursor.isNull(i)) {
						String value = cursor.get(i).toString();
						if (value.isEmpty() && columns[i].getType() == ColumnType.STRING)
							bos.write(EMPTY_STRING);
						else
							bos.write(csvEscape(value, escapeRequired).getBytes(UTF_8));
					}
					terminateCsvField(bos, i == columns.length - 1);
				}
			}
		}
	}

	private static String csvEscape(String field, Matcher escapeRequired) {
		if (escapeRequired.reset(field).find())
			return '"' + field.replace("\"", "\"\"") + '"';
		else
			return field;
	}

	private static void terminateCsvField(BufferedOutputStream bos, boolean last) throws IOException {
		if (last) {
			bos.write('\r');
			bos.write('\n');
		} else
			bos.write(',');
	}

	@Override
	public void writeTo(PreparedStatement ps, WriteToDbConfig config) throws SQLException {
		config.write(this, ps);
	}

	/*--------------------------------------------------------------------------------
	 *	Cell Accessors
	 *--------------------------------------------------------------------------------*/
	@Override
	public boolean isNull(int rowIndex, int columnIndex) {
		Column<?> column = checkedColumn(columnIndex);
		return column.isNull(rowIndex);
	}

	@Override
	public boolean isNull(int rowIndex, String columnName) {
		Column<?> column = checkedColumn(columnName);
		return column.isNull(rowIndex);
	}

	@Override
	public <T> T get(int rowIndex, int columnIndex) {
		Column<?> column = checkedColumn(columnIndex);
		return (T) column.get(rowIndex);
	}

	@Override
	public <T> T get(int rowIndex, String columnName) {
		Column<?> column = checkedColumn(columnName);
		return (T) column.get(rowIndex);
	}

	@Override
	public String getString(int rowIndex, int columnIndex) {
		return stringColumn(columnIndex).get(rowIndex);
	}

	@Override
	public String getString(int rowIndex, String columnName) {
		return stringColumn(columnName).get(rowIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int rowIndex, int columnIndex) {
		return decimalColumn(columnIndex).get(rowIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int rowIndex, String columnName) {
		return decimalColumn(columnName).get(rowIndex);
	}

	@Override
	public boolean getBoolean(int rowIndex, int columnIndex) {
		return booleanColumn(columnIndex).getBoolean(rowIndex);
	}

	@Override
	public boolean getBoolean(int rowIndex, String columnName) {
		return booleanColumn(columnName).getBoolean(rowIndex);
	}

	@Override
	public int getInt(int rowIndex, int columnIndex) {
		return intColumn(columnIndex).getInt(rowIndex);
	}

	@Override
	public int getInt(int rowIndex, String columnName) {
		return intColumn(columnName).getInt(rowIndex);
	}

	@Override
	public long getLong(int rowIndex, int columnIndex) {
		return longColumn(columnIndex).getLong(rowIndex);
	}

	@Override
	public long getLong(int rowIndex, String columnName) {
		return longColumn(columnName).getLong(rowIndex);
	}

	@Override
	public short getShort(int rowIndex, int columnIndex) {
		return shortColumn(columnIndex).getShort(rowIndex);
	}

	@Override
	public short getShort(int rowIndex, String columnName) {
		return shortColumn(columnName).getShort(rowIndex);
	}

	@Override
	public double getDouble(int rowIndex, int columnIndex) {
		return doubleColumn(columnIndex).getDouble(rowIndex);
	}

	@Override
	public double getDouble(int rowIndex, String columnName) {
		return doubleColumn(columnName).getDouble(rowIndex);
	}

	@Override
	public float getFloat(int rowIndex, int columnIndex) {
		return floatColumn(columnIndex).getFloat(rowIndex);
	}

	@Override
	public float getFloat(int rowIndex, String columnName) {
		return floatColumn(columnName).getFloat(rowIndex);
	}

	@Override
	public LocalDate getDate(int rowIndex, int columnIndex) {
		return dateColumn(columnIndex).get(rowIndex);
	}

	@Override
	public LocalDate getDate(int rowIndex, String columnName) {
		return dateColumn(columnName).get(rowIndex);
	}

	@Override
	public int yyyymmdd(int rowIndex, int columnIndex) {
		return dateColumn(columnIndex).yyyymmdd(rowIndex);
	}

	@Override
	public int yyyymmdd(int rowIndex, String columnName) {
		return dateColumn(columnName).yyyymmdd(rowIndex);
	}

	@Override
	public LocalDateTime getDateTime(int rowIndex, int columnIndex) {
		return dateTimeColumn(columnIndex).get(rowIndex);
	}

	@Override
	public LocalDateTime getDateTime(int rowIndex, String columnName) {
		return dateTimeColumn(columnName).get(rowIndex);
	}

	/*--------------------------------------------------------------------------------
	 *	Methods which modify all columns
	 *--------------------------------------------------------------------------------*/
	private DataFrameImpl modifyColumns(Function<AbstractColumn, Column> transformation) {

		Column<?>[] columns = new Column[columnCount()];

		for (int i = 0; i < columns.length; i++) {
			AbstractColumn column = (AbstractColumn) this.columns[i];
			columns[i] = transformation.apply(column);
		}

		return new DataFrameImpl(columns, columnNames, keyIndex, columnToIndexMap);
	}

	private DataFrameImpl filter(BufferBitSet keep) {

		final int cardinality = keep.cardinality();
		if (cardinality == 0)
			return (DataFrameImpl) empty();
		else if (cardinality == size())
			return this;

		return modifyColumns(column -> column.applyFilter(keep, cardinality));
	}

	private DataFrameImpl select(IntColumn indices) {

		if (indices.size() == 0)
			return (DataFrameImpl) empty();

		return modifyColumns(column -> column.select(indices));
	}

	/*--------------------------------------------------------------------------------
	 *	Row Classes
	 *--------------------------------------------------------------------------------*/
	private abstract class AbstractRow implements Row {

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append('[');
			for (int i = 0; i < columnCount(); i++) {
				Object o = get(i);
				sb.append(DEFAULT_PRINTER.pretty(o, columnType(i)));
				if (i < columnCount() - 1)
					sb.append(", ");
			}
			sb.append(']');
			return sb.toString();
		}

		@Override
		public boolean isNull(int columnIndex) {
			return DataFrameImpl.this.isNull(rowIndex(), columnIndex);
		}

		@Override
		public boolean isNull(String columnName) {
			return DataFrameImpl.this.isNull(rowIndex(), columnName);
		}

		@Override
		public <T> T get(int columnIndex) {
			return DataFrameImpl.this.get(rowIndex(), columnIndex);
		}

		@Override
		public <T> T get(String columnName) {
			return DataFrameImpl.this.get(rowIndex(), columnName);
		}

		@Override
		public boolean getBoolean(int columnIndex) {
			return DataFrameImpl.this.getBoolean(rowIndex(), columnIndex);
		}

		@Override
		public boolean getBoolean(String columnName) {
			return DataFrameImpl.this.getBoolean(rowIndex(), columnName);
		}

		@Override
		public int getInt(int columnIndex) {
			return DataFrameImpl.this.getInt(rowIndex(), columnIndex);
		}

		@Override
		public int getInt(String columnName) {
			return DataFrameImpl.this.getInt(rowIndex(), columnName);
		}

		@Override
		public long getLong(int columnIndex) {
			return DataFrameImpl.this.getLong(rowIndex(), columnIndex);
		}

		@Override
		public long getLong(String columnName) {
			return DataFrameImpl.this.getLong(rowIndex(), columnName);
		}

		@Override
		public short getShort(int columnIndex) {
			return DataFrameImpl.this.getShort(rowIndex(), columnIndex);
		}

		@Override
		public short getShort(String columnName) {
			return DataFrameImpl.this.getShort(rowIndex(), columnName);
		}

		@Override
		public double getDouble(int columnIndex) {
			return DataFrameImpl.this.getDouble(rowIndex(), columnIndex);
		}

		@Override
		public double getDouble(String columnName) {
			return DataFrameImpl.this.getDouble(rowIndex(), columnName);
		}

		@Override
		public float getFloat(int columnIndex) {
			return DataFrameImpl.this.getFloat(rowIndex(), columnIndex);
		}

		@Override
		public float getFloat(String columnName) {
			return DataFrameImpl.this.getFloat(rowIndex(), columnName);
		}

		@Override
		public int yyyymmdd(int columnIndex) {
			return DataFrameImpl.this.yyyymmdd(rowIndex(), columnIndex);
		}

		@Override
		public int yyyymmdd(String columnName) {
			return DataFrameImpl.this.yyyymmdd(rowIndex(), columnName);
		}

		@Override
		public int columnCount() {
			return DataFrameImpl.this.columnCount();
		}

		@Override
		public String getString(int columnIndex) {
			return DataFrameImpl.this.getString(rowIndex(), columnIndex);
		}

		@Override
		public String getString(String columnName) {
			return DataFrameImpl.this.getString(rowIndex(), columnName);
		}

		@Override
		public BigDecimal getBigDecimal(int columnIndex) {
			return DataFrameImpl.this.getBigDecimal(rowIndex(), columnIndex);
		}

		@Override
		public BigDecimal getBigDecimal(String columnName) {
			return DataFrameImpl.this.getBigDecimal(rowIndex(), columnName);
		}

		@Override
		public LocalDate getDate(int columnIndex) {
			return DataFrameImpl.this.getDate(rowIndex(), columnIndex);
		}

		@Override
		public LocalDate getDate(String columnName) {
			return DataFrameImpl.this.getDate(rowIndex(), columnName);
		}

		@Override
		public LocalDateTime getDateTime(int columnIndex) {
			return DataFrameImpl.this.getDateTime(rowIndex(), columnIndex);
		}

		@Override
		public LocalDateTime getDateTime(String columnName) {
			return DataFrameImpl.this.getDateTime(rowIndex(), columnName);
		}
	}

	private class RowImpl extends AbstractRow {

		private final int rowIndex;

		private RowImpl(int rowIndex) {
			this.rowIndex = rowIndex;
		}

		@Override
		public int rowIndex() {
			return rowIndex;
		}

		@Override
		public int hashCode() {
			int result = 1;

			for (int i = 0; i < columnCount(); i++) {
				Object value = get(i);
				result = 31 * result + (value == null ? 0 : value.hashCode());
			}

			return result;
		}

		@Override
		public boolean equals(Object o) {
			RowImpl rhs = (RowImpl) o;
			if (columnCount() != rhs.columnCount())
				return false;

			for (int i = 0; i < columnCount(); i++) {
				Object o1 = get(i);
				Object o2 = rhs.get(i);
				if (!(o1 == null ? o2 == null : o1.equals(o2)))
					return false;
			}
			return true;
		}
	}

	private class CursorImpl extends AbstractRow implements Cursor {

		private int rowIndex;

		private CursorImpl(int rowIndex) {
			this.rowIndex = rowIndex;
		}

		@Override
		public int rowIndex() {
			return rowIndex;
		}

		@Override
		public boolean hasNext() {
			return rowIndex < size();
		}

		@Override
		public void next() {
			if (!hasNext())
				throw new NoSuchElementException("called next when hasNext is false");

			rowIndex++;
		}

		@Override
		public boolean hasPrevious() {
			return rowIndex > 0;
		}

		@Override
		public void previous() {
			if (!hasPrevious())
				throw new NoSuchElementException("called previous when hasPrevious is false");

			rowIndex--;
		}
	}
}
