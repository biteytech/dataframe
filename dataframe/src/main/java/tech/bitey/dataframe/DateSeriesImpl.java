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

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Predicate;

import tech.bitey.bufferstuff.BufferBitSet;

public class DateSeriesImpl extends DataFrameImpl implements DateSeries {

	private final NonNullDateColumn dates;
	private final NonNullDoubleColumn values;

	private static LinkedHashMap<String, Column<?>> toColumnMap(DateColumn keys, String dateColumnName,
			DoubleColumn values, String valueColumnName) {
		LinkedHashMap<String, Column<?>> map = new LinkedHashMap<>();
		map.put(dateColumnName, keys);
		map.put(valueColumnName, values);
		return map;
	}

	DateSeriesImpl(NonNullDateColumn dates, String dateColumnName, NonNullDoubleColumn values, String valueColumnName) {
		super(toColumnMap(dates, dateColumnName, values, valueColumnName), dateColumnName);

		this.dates = dates;
		this.values = values;
	}

	@Override
	public DateSeries transform(DoubleUnaryOperator op) {

		DoubleColumnBuilder builder = DoubleColumn.builder();
		builder.ensureCapacity(size());

		for (int i = 0; i < size(); i++)
			builder.add(op.applyAsDouble(values.getDouble(i)));

		return new DateSeriesImpl(dates, columnName(0), (NonNullDoubleColumn) builder.build(), columnName(1));
	}

	@Override
	public DateSeries transform(DateSeries ts, DoubleBinaryOperator op) {

		DateSeriesImpl rhs = (DateSeriesImpl) ts;

		BufferBitSet keepLeft = new BufferBitSet();
		BufferBitSet keepRight = new BufferBitSet();

		int cardinality = dates.intersectBothSorted(rhs.dates, keepLeft, keepRight);

		DoubleColumnBuilder builder = DoubleColumn.builder();
		builder.ensureCapacity(cardinality);

		int l = keepLeft.nextSetBit(0);
		int r = keepRight.nextSetBit(0);
		for (int i = 0; i < cardinality; i++) {

			builder.add(op.applyAsDouble(values.getDouble(l), rhs.values.getDouble(r)));

			l = keepLeft.nextSetBit(l + 1);
			r = keepRight.nextSetBit(r + 1);
		}

		return new DateSeriesImpl((NonNullDateColumn) dates.applyFilter(keepLeft, cardinality), columnName(0),
				(NonNullDoubleColumn) builder.build(), columnName(1));
	}

	@Override
	public Map<LocalDate, Double> asMap() {
		return toMap(1);
	}

	@Override
	public DateSeries sampleN(int sampleSize) {
		return super.sampleN(sampleSize).toDateSeries(1);
	}

	@Override
	public DateSeries sampleX(double proportion) {
		return super.sampleX(proportion).toDateSeries(1);
	}

	@Override
	public DateSeries filter(Predicate<Row> criteria) {
		return super.filter(criteria).toDateSeries(1);
	}

	@Override
	public DateSeries append(DateSeries df) {
		return super.append(df).toDateSeries(1);
	}

	@Override
	public DateSeries head(int count) {
		return super.head(count).toDateSeries(1);
	}

	@Override
	public DateSeries empty() {
		return super.empty().toDateSeries(1);
	}

	@Override
	public DateSeries head() {
		return super.head().toDateSeries(1);
	}

	@Override
	public DateSeries tail(int count) {
		return super.tail(count).toDateSeries(1);
	}

	@Override
	public DateSeries tail() {
		return super.tail().toDateSeries(1);
	}

	@Override
	public DateSeries subSeries(int fromIndex, int toIndex) {
		return super.subFrame(fromIndex, toIndex).toDateSeries(1);
	}

	@Override
	public DateSeries headTo(LocalDate toKey) {
		return super.headTo(toKey).toDateSeries(1);
	}

	@Override
	public DateSeries tailFrom(LocalDate fromKey) {
		return super.tailFrom(fromKey).toDateSeries(1);
	}

	@Override
	public DateSeries subSeries(LocalDate fromKey, LocalDate toKey) {
		return super.subFrameByValue(fromKey, toKey).toDateSeries(1);
	}

	@Override
	public DateSeries subSeries(LocalDate fromKey, boolean fromInclusive, LocalDate toKey, boolean toInclusive) {
		return super.subFrameByValue(fromKey, fromInclusive, toKey, toInclusive).toDateSeries(1);
	}
}
