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
import static tech.bitey.dataframe.LongArrayPacker.LOCAL_TIME;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BigByteBuffer;

final class NonNullTimeColumn extends LongArrayColumn<LocalTime, TimeColumn, NonNullTimeColumn> implements TimeColumn {

	static final Map<Integer, NonNullTimeColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullTimeColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullTimeColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullTimeColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullTimeColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullTimeColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, LOCAL_TIME, offset, size, characteristics, view);
	}

	@Override
	NonNullTimeColumn construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullTimeColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullTimeColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<LocalTime> getType() {
		return ColumnType.TIME;
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof LocalTime;
	}
}
