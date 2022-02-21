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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.ByteColumn;
import tech.bitey.dataframe.DoubleColumn;
import tech.bitey.dataframe.FloatColumn;
import tech.bitey.dataframe.IntColumn;
import tech.bitey.dataframe.LongColumn;
import tech.bitey.dataframe.NormalStringColumn;
import tech.bitey.dataframe.ShortColumn;
import tech.bitey.dataframe.StringColumn;

public class TestClean {

	@Test
	public void clean() {

		List<String> expected;
		StringColumn actual;

		expected = List.of();
		actual = StringColumn.of().clean(s -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of("1", "2", "3");
		actual = StringColumn.of(expected).clean(s -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of("2", "3", "4");
		actual = StringColumn.of("1", "2", "3", "4", "5").subColumn(1, 4).clean(s -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("2", null, "4");
		actual = StringColumn.of("1", "2", "X", "4", "5").subColumn(1, 4).clean(s -> "X".equals(s));
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = StringColumn.of("X", "X", "X").clean(s -> "X".equals(s));
		Assertions.assertEquals(expected, actual);

		expected = new ArrayList<>();
		expected.add(null);
		actual = StringColumn.of(expected).clean(s -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null);
		actual = StringColumn.of(expected).clean(s -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("1", null, null, null, "5", null, "6");
		actual = StringColumn.of("1", null, "X", null, "5", "X", "6").clean(s -> "X".equals(s));
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("3", null, "5");
		actual = StringColumn.of("X", "X", "3", "Y", "5", "X", "X").subColumn(1, 6);
		actual = actual.clean(s -> "X".equals(s));
		actual = actual.subColumn(1, 4);
		actual = actual.clean(s -> "Y".equals(s));
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = StringColumn.of(null, "X", null).clean(s -> "X".equals(s));
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanInt() {

		List<Integer> expected;
		IntColumn actual;

		expected = List.of();
		actual = IntColumn.of().cleanInt(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(1, 2, 3);
		actual = IntColumn.of(expected).cleanInt(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(2, 3, 4);
		actual = IntColumn.of(1, 2, 3, 4, 5).subColumn(1, 4).cleanInt(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2, null, 4);
		actual = IntColumn.of(1, 2, Integer.MIN_VALUE, 4, 5).subColumn(1, 4).cleanInt(x -> Integer.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = IntColumn.of(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE)
				.cleanInt(x -> Integer.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = new ArrayList<>();
		expected.add(null);
		actual = IntColumn.of(expected).cleanInt(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null);
		actual = IntColumn.of(expected).cleanInt(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1, null, null, null, 5, null, 6);
		actual = IntColumn.of(1, null, Integer.MIN_VALUE, null, 5, Integer.MIN_VALUE, 6)
				.cleanInt(x -> Integer.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(3, null, 5);
		actual = IntColumn
				.of(Integer.MIN_VALUE, Integer.MIN_VALUE, 3, Integer.MAX_VALUE, 5, Integer.MIN_VALUE, Integer.MIN_VALUE)
				.subColumn(1, 6);
		actual = actual.cleanInt(x -> Integer.MIN_VALUE == x);
		actual = actual.subColumn(1, 4);
		actual = actual.cleanInt(x -> Integer.MAX_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = IntColumn.of(null, Integer.MIN_VALUE, null).cleanInt(x -> Integer.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanLong() {

		List<Long> expected;
		LongColumn actual;

		expected = List.of();
		actual = LongColumn.of().cleanLong(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(1l, 2l, 3l);
		actual = LongColumn.of(expected).cleanLong(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(2l, 3l, 4l);
		actual = LongColumn.of(1l, 2l, 3l, 4l, 5l).subColumn(1, 4).cleanLong(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2l, null, 4l);
		actual = LongColumn.of(1l, 2l, Long.MIN_VALUE, 4l, 5l).subColumn(1, 4).cleanLong(x -> Long.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = LongColumn.of(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE).cleanLong(x -> Long.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = new ArrayList<>();
		expected.add(null);
		actual = LongColumn.of(expected).cleanLong(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null);
		actual = LongColumn.of(expected).cleanLong(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1l, null, null, null, 5l, null, 6l);
		actual = LongColumn.of(1l, null, Long.MIN_VALUE, null, 5l, Long.MIN_VALUE, 6l)
				.cleanLong(x -> Long.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(3l, null, 5l);
		actual = LongColumn.of(Long.MIN_VALUE, Long.MIN_VALUE, 3l, Long.MAX_VALUE, 5l, Long.MIN_VALUE, Long.MIN_VALUE)
				.subColumn(1, 6);
		actual = actual.cleanLong(x -> Long.MIN_VALUE == x);
		actual = actual.subColumn(1, 4);
		actual = actual.cleanLong(x -> Long.MAX_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = LongColumn.of(null, Long.MIN_VALUE, null).cleanLong(x -> Long.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanDouble() {

		List<Double> expected;
		DoubleColumn actual;

		expected = List.of();
		actual = DoubleColumn.of().cleanDouble(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(1d, 2d, 3d);
		actual = DoubleColumn.of(expected).cleanDouble(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(2d, 3d, 4d);
		actual = DoubleColumn.of(1d, 2d, 3d, 4d, 5d).subColumn(1, 4).cleanDouble(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2d, null, 4d);
		actual = DoubleColumn.of(1d, 2d, Double.MIN_VALUE, 4d, 5d).subColumn(1, 4)
				.cleanDouble(x -> Double.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = DoubleColumn.of(Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE)
				.cleanDouble(x -> Double.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = new ArrayList<>();
		expected.add(null);
		actual = DoubleColumn.of(expected).cleanDouble(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null);
		actual = DoubleColumn.of(expected).cleanDouble(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1d, null, null, null, 5d, null, 6d);
		actual = DoubleColumn.of(1d, null, Double.MIN_VALUE, null, 5d, Double.MIN_VALUE, 6d)
				.cleanDouble(x -> Double.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(3d, null, 5d);
		actual = DoubleColumn
				.of(Double.MIN_VALUE, Double.MIN_VALUE, 3d, Double.MAX_VALUE, 5d, Double.MIN_VALUE, Double.MIN_VALUE)
				.subColumn(1, 6);
		actual = actual.cleanDouble(x -> Double.MIN_VALUE == x);
		actual = actual.subColumn(1, 4);
		actual = actual.cleanDouble(x -> Double.MAX_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = DoubleColumn.of(null, Double.MIN_VALUE, null).cleanDouble(x -> Double.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanNaN() {

		List<Double> expected = DoubleColumn.of(1d, null, 3d);
		DoubleColumn actual = DoubleColumn.of(1d, Double.NaN, 3d).cleanNaN();
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanFloat() {

		List<Float> expected;
		FloatColumn actual;

		expected = List.of();
		actual = FloatColumn.of().cleanFloat(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(1f, 2f, 3f);
		actual = FloatColumn.of(expected).cleanFloat(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of(2f, 3f, 4f);
		actual = FloatColumn.of(1f, 2f, 3f, 4f, 5f).subColumn(1, 4).cleanFloat(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2f, null, 4f);
		actual = FloatColumn.of(1f, 2f, Float.MIN_VALUE, 4f, 5f).subColumn(1, 4).cleanFloat(x -> Float.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = FloatColumn.of(Float.MIN_VALUE, Float.MIN_VALUE, Float.MIN_VALUE)
				.cleanFloat(x -> Float.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = new ArrayList<>();
		expected.add(null);
		actual = FloatColumn.of(expected).cleanFloat(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null);
		actual = FloatColumn.of(expected).cleanFloat(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1f, null, null, null, 5f, null, 6f);
		actual = FloatColumn.of(1f, null, Float.MIN_VALUE, null, 5f, Float.MIN_VALUE, 6f)
				.cleanFloat(x -> Float.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(3f, null, 5f);
		actual = FloatColumn
				.of(Float.MIN_VALUE, Float.MIN_VALUE, 3f, Float.MAX_VALUE, 5f, Float.MIN_VALUE, Float.MIN_VALUE)
				.subColumn(1, 6);
		actual = actual.cleanFloat(x -> Float.MIN_VALUE == x);
		actual = actual.subColumn(1, 4);
		actual = actual.cleanFloat(x -> Float.MAX_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = FloatColumn.of(null, Float.MIN_VALUE, null).cleanFloat(x -> Float.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanShort() {

		List<Short> expected;
		ShortColumn actual;

		expected = List.of();
		actual = ShortColumn.of().cleanShort(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of((short) 1, (short) 2, (short) 3);
		actual = ShortColumn.of(expected).cleanShort(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of((short) 2, (short) 3, (short) 4);
		actual = ShortColumn.of((short) 1, (short) 2, (short) 3, (short) 4, (short) 5).subColumn(1, 4)
				.cleanShort(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList((short) 2, null, (short) 4);
		actual = ShortColumn.of((short) 1, (short) 2, Short.MIN_VALUE, (short) 4, (short) 5).subColumn(1, 4)
				.cleanShort(x -> Short.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = ShortColumn.of(Short.MIN_VALUE, Short.MIN_VALUE, Short.MIN_VALUE)
				.cleanShort(x -> Short.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = new ArrayList<>();
		expected.add(null);
		actual = ShortColumn.of(expected).cleanShort(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null);
		actual = ShortColumn.of(expected).cleanShort(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList((short) 1, null, null, null, (short) 5, null, (short) 6);
		actual = ShortColumn.of((short) 1, null, Short.MIN_VALUE, null, (short) 5, Short.MIN_VALUE, (short) 6)
				.cleanShort(x -> Short.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList((short) 3, null, (short) 5);
		actual = ShortColumn.of(Short.MIN_VALUE, Short.MIN_VALUE, (short) 3, Short.MAX_VALUE, (short) 5,
				Short.MIN_VALUE, Short.MIN_VALUE).subColumn(1, 6);
		actual = actual.cleanShort(x -> Short.MIN_VALUE == x);
		actual = actual.subColumn(1, 4);
		actual = actual.cleanShort(x -> Short.MAX_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = ShortColumn.of(null, Short.MIN_VALUE, null).cleanShort(x -> Short.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanByte() {

		List<Byte> expected;
		ByteColumn actual;

		expected = List.of();
		actual = ByteColumn.of().cleanByte(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of((byte) 1, (byte) 2, (byte) 3);
		actual = ByteColumn.of(expected).cleanByte(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = List.of((byte) 2, (byte) 3, (byte) 4);
		actual = ByteColumn.of((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5).subColumn(1, 4).cleanByte(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList((byte) 2, null, (byte) 4);
		actual = ByteColumn.of((byte) 1, (byte) 2, Byte.MIN_VALUE, (byte) 4, (byte) 5).subColumn(1, 4)
				.cleanByte(x -> Byte.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = ByteColumn.of(Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE).cleanByte(x -> Byte.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = new ArrayList<>();
		expected.add(null);
		actual = ByteColumn.of(expected).cleanByte(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null);
		actual = ByteColumn.of(expected).cleanByte(x -> false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList((byte) 1, null, null, null, (byte) 5, null, (byte) 6);
		actual = ByteColumn.of((byte) 1, null, Byte.MIN_VALUE, null, (byte) 5, Byte.MIN_VALUE, (byte) 6)
				.cleanByte(x -> Byte.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList((byte) 3, null, (byte) 5);
		actual = ByteColumn
				.of(Byte.MIN_VALUE, Byte.MIN_VALUE, (byte) 3, Byte.MAX_VALUE, (byte) 5, Byte.MIN_VALUE, Byte.MIN_VALUE)
				.subColumn(1, 6);
		actual = actual.cleanByte(x -> Byte.MIN_VALUE == x);
		actual = actual.subColumn(1, 4);
		actual = actual.cleanByte(x -> Byte.MAX_VALUE == x);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, null, null);
		actual = ByteColumn.of(null, Byte.MIN_VALUE, null).cleanByte(x -> Byte.MIN_VALUE == x);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void cleanNormalString() {

		List<String> expected;
		NormalStringColumn actual;

		expected = Arrays.asList("a", null, null, "c", "c", null);
		actual = NormalStringColumn.of("a", "a", "b", "b", "c", "c", "b", "b").subColumn(1, 7)
				.clean(s -> "b".equals(s));
		Assertions.assertEquals(expected, actual);
	}
}
