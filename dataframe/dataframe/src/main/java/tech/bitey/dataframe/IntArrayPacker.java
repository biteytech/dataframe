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

package tech.bitey.dataframe;

import java.time.LocalDate;

interface IntArrayPacker<E> {

	final IntArrayPacker<Integer> INTEGER = new IntArrayPacker<Integer>() {
		@Override
		public int pack(Integer value) {
			return value.intValue();
		}

		@Override
		public Integer unpack(int packed) {
			return Integer.valueOf(packed);
		}
	};

	final IntArrayPacker<LocalDate> LOCAL_DATE = new IntArrayPacker<LocalDate>() {

		@Override
		public int pack(LocalDate value) {
			int packed = IntArrayPacker.packDate(value.getYear(), value.getMonthValue(), value.getDayOfMonth());
			return packed;
		}

		@Override
		public LocalDate unpack(int packed) {
			return LocalDate.of(packed >> 9, (packed & 0x1E0) >> 5, packed & 0x1F);
		}
	};

	int pack(E value);

	E unpack(int packed);

	public static int packDate(int year, int month, int day) {
		return year << 9 | month << 5 | day;
	}
}
