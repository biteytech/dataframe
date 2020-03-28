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

import java.math.BigDecimal;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNumericColumn {

	private static final double DELTA = 1e-10;

	@Test
	public void numericMethods() {
		test(new int[] { 0 });
		test(new int[] { 1 });
		test(new int[] { 0, 0 });
		test(new int[] { 1, 1 });
		test(new int[] { -1, 1 });
		test(new int[] { 1, 2 });
		test(new int[] { 1, 2, 3 });
		test(new int[] { -1, -2, -3 });
		test(new int[] { 2, 4, 6, 8 });
		test(new int[] { -2, -4, -6, -8 });
		test(new int[] { 80, 72, 95, 27, 56, 71, 73, 92, 37, 84 });
		test(new int[] { 20000, -10000, 0, 1, 10000 });
		test(new int[] { Byte.MAX_VALUE, Byte.MIN_VALUE });
		test(new int[] { Short.MAX_VALUE, Short.MIN_VALUE });
		test(new int[] { 2000000000, -1000000000, 0, 1, 1000000000 });
		test(new int[] { Integer.MAX_VALUE / 100, Integer.MIN_VALUE / 100 });
	}

	private static void test(int[] values) {

		double min = Arrays.stream(values).min().getAsInt();
		double max = Arrays.stream(values).max().getAsInt();
		double mean = Arrays.stream(values).average().getAsDouble();

		double numer = Arrays.stream(values).asDoubleStream().map(v -> v - mean).map(v -> v * v).sum();
		double population = Math.sqrt(numer / values.length);
		double sample = values.length == 1 ? Double.NaN : Math.sqrt(numer / (values.length - 1));

		test("Int", IntColumn.of(toInteger(values)), min, max, mean, population, sample);
		test("Long", LongColumn.of(toLong(values)), min, max, mean, population, sample);
		test("Double", DoubleColumn.of(toDouble(values)), min, max, mean, population, sample);
		test("Float", FloatColumn.of(toFloat(values)), min, max, mean, population, sample);
		test("Decimal", DecimalColumn.of(toBigDecimal(values)), min, max, mean, population, sample);

		if (values[0] <= Short.MAX_VALUE)
			test("Short", ShortColumn.of(toShort(values)), min, max, mean, population, sample);
		if (values[0] <= Byte.MAX_VALUE)
			test("Byte", ByteColumn.of(toByte(values)), min, max, mean, population, sample);
	}

	private static void test(String columnType, NumericColumn<?> column, double min, double max, double mean,
			double population, double sample) {

		Assertions.assertEquals(min, column.min(), columnType + ", min");
		Assertions.assertEquals(max, column.max(), columnType + ", max");
		Assertions.assertEquals(mean, column.mean(), DELTA, columnType + ", mean");
		Assertions.assertEquals(population, column.stddev(true), DELTA, columnType + ", population");
		Assertions.assertEquals(sample, column.stddev(), DELTA, columnType + ", sample");
	}

	private static Integer[] toInteger(int[] values) {
		Integer[] array = new Integer[values.length];
		for (int i = 0; i < values.length; i++)
			array[i] = values[i];
		return array;
	}

	private static Long[] toLong(int[] values) {
		Long[] array = new Long[values.length];
		for (int i = 0; i < values.length; i++)
			array[i] = (long) values[i];
		return array;
	}

	private static Short[] toShort(int[] values) {
		Short[] array = new Short[values.length];
		for (int i = 0; i < values.length; i++)
			array[i] = (short) values[i];
		return array;
	}

	private static Byte[] toByte(int[] values) {
		Byte[] array = new Byte[values.length];
		for (int i = 0; i < values.length; i++)
			array[i] = (byte) values[i];
		return array;
	}

	private static Double[] toDouble(int[] values) {
		Double[] array = new Double[values.length];
		for (int i = 0; i < values.length; i++)
			array[i] = (double) values[i];
		return array;
	}

	private static Float[] toFloat(int[] values) {
		Float[] array = new Float[values.length];
		for (int i = 0; i < values.length; i++)
			array[i] = (float) values[i];
		return array;
	}

	private static BigDecimal[] toBigDecimal(int[] values) {
		BigDecimal[] array = new BigDecimal[values.length];
		for (int i = 0; i < values.length; i++)
			array[i] = BigDecimal.valueOf(values[i]);
		return array;
	}
}
