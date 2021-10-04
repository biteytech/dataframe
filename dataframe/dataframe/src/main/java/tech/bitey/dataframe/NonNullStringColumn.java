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
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

final class NonNullStringColumn extends NonNullVarLenColumn<String, StringColumn, NonNullStringColumn>
		implements StringColumn {

	static final Map<Integer, NonNullStringColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS,
				c -> new NonNullStringColumn(EMPTY_BUFFER, EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullStringColumn(EMPTY_BUFFER, EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullStringColumn(EMPTY_BUFFER, EMPTY_BUFFER, 0, 0, c, false));
	}

	static NonNullStringColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullStringColumn(ByteBuffer elements, ByteBuffer rawPointers, int offset, int size, int characteristics,
			boolean view) {
		super(VarLenPacker.STRING, elements, rawPointers, offset, size, characteristics, view);
	}

	@Override
	NonNullStringColumn construct(ByteBuffer elements, ByteBuffer rawPointers, int offset, int size,
			int characteristics, boolean view) {
		return new NonNullStringColumn(elements, rawPointers, offset, size, characteristics, view);
	}

	@Override
	NonNullStringColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.STRING;
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof String;
	}

	@Override
	NonNullStringColumn toSorted0() {
		// TODO: make this more efficient
		return (NonNullStringColumn) stream().sorted().collect(StringColumn.collector(NONNULL | SORTED));
	}

	@Override
	NonNullStringColumn toDistinct0(boolean sort) {
		// TODO: make this more efficient
		Stream<String> stream = stream();
		if (sort)
			stream = stream.sorted();
		return (NonNullStringColumn) stream.distinct().collect(StringColumn.collector(NONNULL | SORTED | DISTINCT));
	}
}
