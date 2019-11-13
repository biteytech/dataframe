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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

final class NonNullDecimalColumn extends NonNullVarLenColumn<BigDecimal, DecimalColumn, NonNullDecimalColumn>
		implements DecimalColumn {

	static final Map<Integer, NonNullDecimalColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS,
				c -> new NonNullDecimalColumn(EMPTY_BUFFER, EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullDecimalColumn(EMPTY_BUFFER, EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullDecimalColumn(EMPTY_BUFFER, EMPTY_BUFFER, 0, 0, c, false));
	}

	static NonNullDecimalColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullDecimalColumn(ByteBuffer elements, ByteBuffer rawPointers, int offset, int size, int characteristics,
			boolean view) {
		super(VarLenPacker.DECIMAL, elements, rawPointers, offset, size, characteristics, view);
	}

	@Override
	NonNullDecimalColumn construct(ByteBuffer elements, ByteBuffer rawPointers, int offset, int size,
			int characteristics, boolean view) {
		return new NonNullDecimalColumn(elements, rawPointers, offset, size, characteristics, view);
	}

	@Override
	NonNullDecimalColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public Comparator<BigDecimal> comparator() {
		return BigDecimal::compareTo;
	}

	@Override
	public ColumnType getType() {
		return ColumnType.DECIMAL;
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof BigDecimal;
	}

	@Override
	NonNullDecimalColumn toSorted0() {
		DecimalColumnBuilder builder = new DecimalColumnBuilder(NONNULL);
		builder.addAll(this);
		builder.sort();
		return (NonNullDecimalColumn) builder.build();
	}

	@Override
	NonNullDecimalColumn toDistinct0(boolean sort) {
		DecimalColumnBuilder builder = new DecimalColumnBuilder(NONNULL);
		builder.addAll(this);
		builder.distinct();
		return (NonNullDecimalColumn) builder.build();
	}

	private double dat(int index) {
		return getNoOffset(index).doubleValue();
	}

	@Override
	public double min() {
		if (size == 0)
			return Double.NaN;
		else if (isSorted())
			return dat(offset);

		double min = dat(offset);

		for (int i = offset + 1; i <= lastIndex(); i++) {
			double x = dat(i);
			if (x < min)
				min = x;
		}

		return min;
	}

	@Override
	public double max() {
		if (size == 0)
			return Double.NaN;
		else if (isSorted())
			return dat(lastIndex());

		double max = dat(offset);

		for (int i = offset + 1; i <= lastIndex(); i++) {
			double x = dat(i);
			if (x > max)
				max = x;
		}

		return max;
	}

	@Override
	public double mean() {
		if (size == 0)
			return Double.NaN;

		double sum = 0;
		for (int i = offset; i <= lastIndex(); i++)
			sum += dat(i);

		// Compute initial estimate using definitional formula
		double xbar = sum / size;

		// Compute correction factor in second pass
		double correction = 0;
		for (int i = offset; i <= lastIndex(); i++) {
			correction += dat(i) - xbar;
		}
		return xbar + (correction / size);
	}

	@Override
	public double stddev(boolean population) {
		if (size == 0 || (!population && size == 1))
			return Double.NaN;

		double μ = mean();

		double numer = 0;
		for (int i = offset; i <= lastIndex(); i++) {
			double d = dat(i) - μ;
			numer += d * d;
		}

		return Math.sqrt(numer / (population ? size : size - 1));
	}
}
