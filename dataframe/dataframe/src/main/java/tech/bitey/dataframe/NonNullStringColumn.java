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

import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BigByteBuffer;

final class NonNullStringColumn extends NonNullVarLenColumn<String, StringColumn, NonNullStringColumn>
		implements StringColumn {

	static final Map<Integer, NonNullStringColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS,
				c -> new NonNullStringColumn(EMPTY_BIG_BUFFER, EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullStringColumn(EMPTY_BIG_BUFFER, EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullStringColumn(EMPTY_BIG_BUFFER, EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullStringColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullStringColumn(BigByteBuffer elements, BigByteBuffer rawPointers, int offset, int size, int characteristics,
			boolean view) {
		super(VarLenPacker.STRING, elements, rawPointers, offset, size, characteristics, view);
	}

	@Override
	NonNullStringColumn construct(BigByteBuffer elements, BigByteBuffer rawPointers, int offset, int size,
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
}
