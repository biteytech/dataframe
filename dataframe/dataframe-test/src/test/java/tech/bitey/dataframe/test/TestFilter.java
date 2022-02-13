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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.DoubleColumn;
import tech.bitey.dataframe.IntColumn;
import tech.bitey.dataframe.LongColumn;
import tech.bitey.dataframe.NormalStringColumn;
import tech.bitey.dataframe.StringColumn;

public class TestFilter {

	@Test
	public void filter() {

		List<String> expected;
		StringColumn actual;

		expected = Arrays.asList();
		actual = StringColumn.of().filter(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = StringColumn.of("1").filter(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = StringColumn.of("1", "2").filter(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("1", "2");
		actual = StringColumn.of("1", "2").filter(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("1", "3");
		actual = StringColumn.of("1", "2", "3").filter(s -> !"2".equals(s), false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("2");
		actual = StringColumn.of("1", "2", "3").filter(s -> "2".equals(s), false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = StringColumn.builder().addNull().build().filter(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = StringColumn.of("1", null).filter(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = StringColumn.of("1", "2", null).filter(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("1", "2");
		actual = StringColumn.of("1", "2", null).filter(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("1", "3");
		actual = StringColumn.of("1", null, "2", "3", null).filter(s -> !"2".equals(s), false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("2");
		actual = StringColumn.of("1", "2", "3", null).filter(s -> "2".equals(s), false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new String[] { null });
		actual = StringColumn.builder().addNull().build().filter(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new String[] { null });
		actual = StringColumn.of("1", null).filter(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new String[] { null });
		actual = StringColumn.of("1", "2", null).filter(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("1", "2", null);
		actual = StringColumn.of("1", "2", null).filter(s -> true, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("1", null, "3", null);
		actual = StringColumn.of("1", null, "2", "3", null).filter(s -> !"2".equals(s), true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList("2", null);
		actual = StringColumn.of("1", "2", "3", null).filter(s -> "2".equals(s), true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, "3");
		actual = StringColumn.of("1", null, "2", "3", null).subColumn(1, 4).filter(s -> !"2".equals(s), true);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void filterNormalString() {

		List<String> expected;
		NormalStringColumn actual;

		expected = Arrays.asList("a", "c", "c");
		actual = NormalStringColumn.of("a", "a", "b", "b", "c", "c", "b", "b").subColumn(1, 7)
				.filter(s -> !"b".equals(s), false);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testFilterInt() {

		List<Integer> expected;
		IntColumn actual;

		expected = Arrays.asList();
		actual = IntColumn.of().filterInt(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = IntColumn.of(1).filterInt(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = IntColumn.of(1, 2).filterInt(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1, 2);
		actual = IntColumn.of(1, 2).filterInt(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1, 3);
		actual = IntColumn.of(1, 2, 3).filterInt(i -> i != 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2);
		actual = IntColumn.of(1, 2, 3).filterInt(i -> i == 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = IntColumn.builder().addNull().build().filterInt(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = IntColumn.of(1, null).filterInt(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = IntColumn.of(1, 2, null).filterInt(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1, 2);
		actual = IntColumn.of(1, 2, null).filterInt(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1, 3);
		actual = IntColumn.of(1, null, 2, 3, null).filterInt(i -> i != 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2);
		actual = IntColumn.of(1, 2, 3, null).filterInt(i -> i == 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Integer[] { null });
		actual = IntColumn.builder().addNull().build().filterInt(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Integer[] { null });
		actual = IntColumn.of(1, null).filterInt(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Integer[] { null });
		actual = IntColumn.of(1, 2, null).filterInt(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1, 2, null);
		actual = IntColumn.of(1, 2, null).filterInt(s -> true, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1, null, 3, null);
		actual = IntColumn.of(1, null, 2, 3, null).filterInt(i -> i != 2, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2, null);
		actual = IntColumn.of(1, 2, 3, null).filterInt(i -> i == 2, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, 3);
		actual = IntColumn.of(1, null, 2, 3, null).subColumn(1, 4).filterInt(i -> i != 2, true);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testFilterLong() {

		List<Long> expected;
		LongColumn actual;

		expected = Arrays.asList();
		actual = LongColumn.of().filterLong(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = LongColumn.of(1l).filterLong(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = LongColumn.of(1l, 2l).filterLong(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1l, 2l);
		actual = LongColumn.of(1l, 2l).filterLong(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1l, 3l);
		actual = LongColumn.of(1l, 2l, 3l).filterLong(i -> i != 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2l);
		actual = LongColumn.of(1l, 2l, 3l).filterLong(i -> i == 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = LongColumn.builder().addNull().build().filterLong(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = LongColumn.of(1l, null).filterLong(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = LongColumn.of(1l, 2l, null).filterLong(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1l, 2l);
		actual = LongColumn.of(1l, 2l, null).filterLong(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1l, 3l);
		actual = LongColumn.of(1l, null, 2l, 3l, null).filterLong(i -> i != 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2l);
		actual = LongColumn.of(1l, 2l, 3l, null).filterLong(i -> i == 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Long[] { null });
		actual = LongColumn.builder().addNull().build().filterLong(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Long[] { null });
		actual = LongColumn.of(1l, null).filterLong(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Long[] { null });
		actual = LongColumn.of(1l, 2l, null).filterLong(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1l, 2l, null);
		actual = LongColumn.of(1l, 2l, null).filterLong(s -> true, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1l, null, 3l, null);
		actual = LongColumn.of(1l, null, 2l, 3l, null).filterLong(i -> i != 2, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2l, null);
		actual = LongColumn.of(1l, 2l, 3l, null).filterLong(i -> i == 2, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, 3l);
		actual = LongColumn.of(1l, null, 2l, 3l, null).subColumn(1, 4).filterLong(i -> i != 2, true);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testFilterDouble() {

		List<Double> expected;
		DoubleColumn actual;

		expected = Arrays.asList();
		actual = DoubleColumn.of().filterDouble(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = DoubleColumn.of(1d).filterDouble(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = DoubleColumn.of(1d, 2d).filterDouble(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1d, 2d);
		actual = DoubleColumn.of(1d, 2d).filterDouble(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1d, 3d);
		actual = DoubleColumn.of(1d, 2d, 3d).filterDouble(i -> i != 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2d);
		actual = DoubleColumn.of(1d, 2d, 3d).filterDouble(i -> i == 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = DoubleColumn.builder().addNull().build().filterDouble(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = DoubleColumn.of(1d, null).filterDouble(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList();
		actual = DoubleColumn.of(1d, 2d, null).filterDouble(s -> false, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1d, 2d);
		actual = DoubleColumn.of(1d, 2d, null).filterDouble(s -> true, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1d, 3d);
		actual = DoubleColumn.of(1d, null, 2d, 3d, null).filterDouble(i -> i != 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2d);
		actual = DoubleColumn.of(1d, 2d, 3d, null).filterDouble(i -> i == 2, false);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Double[] { null });
		actual = DoubleColumn.builder().addNull().build().filterDouble(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Double[] { null });
		actual = DoubleColumn.of(1d, null).filterDouble(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(new Double[] { null });
		actual = DoubleColumn.of(1d, 2d, null).filterDouble(s -> false, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1d, 2d, null);
		actual = DoubleColumn.of(1d, 2d, null).filterDouble(s -> true, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(1d, null, 3d, null);
		actual = DoubleColumn.of(1d, null, 2d, 3d, null).filterDouble(i -> i != 2, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(2d, null);
		actual = DoubleColumn.of(1d, 2d, 3d, null).filterDouble(i -> i == 2, true);
		Assertions.assertEquals(expected, actual);

		expected = Arrays.asList(null, 3d);
		actual = DoubleColumn.of(1d, null, 2d, 3d, null).subColumn(1, 4).filterDouble(i -> i != 2, true);
		Assertions.assertEquals(expected, actual);
	}
}
