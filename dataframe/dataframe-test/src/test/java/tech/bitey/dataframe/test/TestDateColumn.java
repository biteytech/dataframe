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

import java.time.LocalDate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.DateColumn;

public class TestDateColumn {

	@Test
	public void now() {

		LocalDate expected = LocalDate.now();
		LocalDate actual = packUnpack(expected);

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void largeYears() {

		int[] years = { -(1 << 22), -99999, 99999, (1 << 22) - 1 };

		for (int year : years) {

			LocalDate expected = LocalDate.of(year, 1, 1);
			LocalDate actual = packUnpack(expected);

			Assertions.assertEquals(expected, actual);
		}
	}

	private static LocalDate packUnpack(LocalDate date) {
		return DateColumn.of(date).get(0);
	}
}
