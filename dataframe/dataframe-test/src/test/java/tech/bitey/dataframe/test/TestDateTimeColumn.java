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

import java.time.LocalDateTime;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.DateTimeColumn;

public class TestDateTimeColumn {

	@Test
	public void now() {

		LocalDateTime expected = LocalDateTime.now();
		expected = expected.withNano(expected.getNano() / 1000 * 1000);
		LocalDateTime actual = packUnpack(expected);

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void largeYears() {

		int[] years = { -(1 << 17), -99999, 99999, (1 << 17) - 1 };

		for (int year : years) {

			LocalDateTime expected = LocalDateTime.of(year, 1, 1, 0, 0);
			LocalDateTime actual = packUnpack(expected);

			Assertions.assertEquals(expected, actual);
		}
	}

	@Test
	public void testNano() {

		int[] nanos = { 0, 1, 999, 1000, 1001, 111111111, 123456789, 999999999 };

		for (int nano : nanos) {

			LocalDateTime in = LocalDateTime.of(2000, 1, 1, 0, 0, 0, nano);
			LocalDateTime out = packUnpack(in);

			Assertions.assertEquals(nano / 1000 * 1000, out.getNano());
		}
	}

	private static LocalDateTime packUnpack(LocalDateTime dt) {
		return DateTimeColumn.of(dt).get(0);
	}
}
