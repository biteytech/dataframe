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

import java.time.LocalDateTime;
import java.time.LocalTime;

interface LongArrayPacker<E> {

	final LongArrayPacker<Long> LONG = new LongArrayPacker<Long>() {
		@Override
		public long pack(Long value) {
			return value.longValue();
		}

		@Override
		public Long unpack(long packed) {
			return Long.valueOf(packed);
		}
	};

	/**
	 * Years can range from -131072 (-2^17) to 131071 (2^17-1). Microsecond
	 * precision (last 3 digits of nanosecond field are always 0).
	 */
	final LongArrayPacker<LocalDateTime> LOCAL_DATE_TIME = new LongArrayPacker<LocalDateTime>() {
		@Override
		public long pack(LocalDateTime value) {

			long packed = (long) value.getYear() << 46 | (long) value.getMonthValue() << 42
					| (long) value.getDayOfMonth() << 37 | (long) value.getHour() << 32 | (long) value.getMinute() << 26
					| value.getSecond() << 20 | value.getNano() / 1000;

			return packed;
		}

		@Override
		public LocalDateTime unpack(long packed) {

			return LocalDateTime.of((int) (packed >> 46), // year
					(int) ((packed & 0x3C0000000000L) >> 42), // month
					(int) ((packed & 0x3E000000000L) >> 37), // dayOfMonth
					(int) ((packed & 0x1F00000000L) >> 32), // hour
					(int) ((packed & 0xFC000000L) >> 26), // minute
					(int) ((packed & 0x3F00000L) >> 20), // second
					(int) ((packed & 0xFFFFF) * 1000) // nanoOfSecond
			);
		}
	};

	final LongArrayPacker<LocalTime> LOCAL_TIME = new LongArrayPacker<LocalTime>() {
		@Override
		public long pack(LocalTime value) {

			long packed = (long) value.getHour() << 42 | (long) value.getMinute() << 36 | (long) value.getSecond() << 30
					| value.getNano();

			return packed;
		}

		@Override
		public LocalTime unpack(long packed) {

			return LocalTime.of((int) (packed >> 42), // hour
					(int) ((packed & 0x3F000000000L) >> 36), // minute
					(int) ((packed & 0xFC0000000L) >> 30), // second
					(int) ((packed & 0x3FFFFFFF)) // nanoOfSecond
			);
		}
	};

	long pack(E value);

	E unpack(long packed);
}
