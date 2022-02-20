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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.ByteColumn;
import tech.bitey.dataframe.DoubleColumn;
import tech.bitey.dataframe.FloatColumn;
import tech.bitey.dataframe.IntColumn;
import tech.bitey.dataframe.LongColumn;
import tech.bitey.dataframe.ShortColumn;

public class TestEvaluate {

	@Test
	public void testEvaluateInt() {

		IntColumn arg = IntColumn.of(null, null, 1, 2, null, 3, null, null).subColumn(1, 7);
		IntColumn expected = arg.toIntColumn(i -> i + i);
		IntColumn actual = arg.evaluate(i -> i + i);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testEvaluateLong() {

		LongColumn arg = LongColumn.of(null, null, 1l, 2l, null, 3l, null, null).subColumn(1, 7);
		LongColumn expected = arg.toLongColumn(i -> i + i);
		LongColumn actual = arg.evaluate(i -> i + i);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testEvaluateDouble() {

		DoubleColumn arg = DoubleColumn.of(null, null, 1d, 2d, null, 3d, null, null).subColumn(1, 7);
		DoubleColumn expected = arg.toDoubleColumn(i -> i + i);
		DoubleColumn actual = arg.evaluate(i -> i + i);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testEvaluateByte() {

		ByteColumn arg = ByteColumn.of(null, null, (byte) 1, (byte) 2, null, (byte) 3, null, null).subColumn(1, 7);
		ByteColumn expected = arg.toByteColumn(i -> (byte) (i + i));
		ByteColumn actual = arg.evaluate(i -> (byte) (i + i));
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testEvaluateShort() {

		ShortColumn arg = ShortColumn.of(null, null, (short) 1, (short) 2, null, (short) 3, null, null).subColumn(1, 7);
		ShortColumn expected = arg.toShortColumn(i -> (short) (i + i));
		ShortColumn actual = arg.evaluate(i -> (short) (i + i));
		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void testEvaluateFloat() {

		FloatColumn arg = FloatColumn.of(null, null, 1f, 2f, null, 3f, null, null).subColumn(1, 7);
		FloatColumn expected = arg.toFloatColumn(i -> i + i);
		FloatColumn actual = arg.evaluate(i -> i + i);
		Assertions.assertEquals(expected, actual);
	}
}
