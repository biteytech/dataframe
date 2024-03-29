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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BIG_BUFFER;
import static tech.bitey.dataframe.LongArrayPacker.LOCAL_DATE_TIME;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BigByteBuffer;

final class NonNullDateTimeColumn extends LongArrayColumn<LocalDateTime, DateTimeColumn, NonNullDateTimeColumn>
		implements DateTimeColumn {

	static final Map<Integer, NonNullDateTimeColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS,
				c -> new NonNullDateTimeColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullDateTimeColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullDateTimeColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullDateTimeColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullDateTimeColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, LOCAL_DATE_TIME, offset, size, characteristics, view);
	}

	@Override
	NonNullDateTimeColumn construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullDateTimeColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullDateTimeColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<LocalDateTime> getType() {
		return ColumnType.DATETIME;
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof LocalDateTime;
	}
}
