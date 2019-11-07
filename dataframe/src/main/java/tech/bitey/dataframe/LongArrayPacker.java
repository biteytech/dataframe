/*
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

interface LongArrayPacker<E> {

	final LongArrayPacker<Long> LONG = new LongArrayPacker<Long>() {
		@Override
		public long pack(Long value) {
			return value.longValue();
		}

		@Override
		public Long unpack(long packed) {
			return new Long(packed);
		}
	};
	
	final LongArrayPacker<LocalDateTime> LOCAL_DATE_TIME = new LongArrayPacker<LocalDateTime>() {
		@Override
		public long pack(LocalDateTime value) {
			
			long packed = (long)value.getYear() << 47 | (long)value.getMonthValue() << 43 | (long)value.getDayOfMonth() << 37
					| (long)value.getHour() << 32 | (long)value.getMinute() << 26 | value.getSecond() << 20 | value.getNano()/1000;
			
			return packed;
		}

		@Override
		public LocalDateTime unpack(long packed) {
			
			return LocalDateTime.of(
					(int)((packed & 0x7FF800000000000L) >> 47), // year
					(int)((packed & 0x780000000000L) >> 43), // month
					(int)((packed & 0x7E000000000L) >> 37), // dayOfMonth
					(int)((packed & 0x1F00000000L) >> 32), // hour
					(int)((packed & 0xFC000000L) >> 26), // minute
					(int)((packed & 0x3F00000L) >> 20), // second
					(int)((packed & 0xFFFFF)*1000) // nanoOfSecond
			);
		}
	};
	
	long pack(E value);
	E unpack(long packed);
}
