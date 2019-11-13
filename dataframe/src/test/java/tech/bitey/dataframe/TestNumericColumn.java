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
		test(new int[] { 2000000000, -1000000000, 0, 1, 1000000000 });
		test(new int[] { Integer.MAX_VALUE/100, Integer.MIN_VALUE/100 });
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
