/*
 * Copyright 2021 biteytech@protonmail.com
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
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.dataframe.Pr.checkElementIndex;
import static tech.bitey.dataframe.LongArrayPacker.LONG;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import tech.bitey.bufferstuff.BufferUtils;

final class NonNullLongColumn extends LongArrayColumn<Long, LongColumn, NonNullLongColumn> implements LongColumn {

	static final Map<Integer, NonNullLongColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullLongColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullLongColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullLongColumn(EMPTY_BUFFER, 0, 0, c, false));
	}

	static NonNullLongColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullLongColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, LONG, offset, size, characteristics, view);
	}

	@Override
	NonNullLongColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullLongColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullLongColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<Long> getType() {
		return ColumnType.LONG;
	}

	@Override
	public long getLong(int index) {
		checkElementIndex(index, size);
		return at(index + offset);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Long;
	}

	@Override
	public LongStream longStream() {
		return BufferUtils.stream(elements, offset, offset + size, characteristics);
	}
}
