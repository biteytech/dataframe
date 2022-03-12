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
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
		super(checkIsAscii(elements) ? VarLenPacker.STRING_ASCII : VarLenPacker.STRING_UTF_8, elements, rawPointers,
				offset, size, characteristics, view);
	}

	private static boolean checkIsAscii(ByteBuffer elements) {
		for (int i = 0; i < elements.limit(); i++)
			if (elements.get(i) < 0)
				return false;

		return true;
	}

	private boolean isAscii() {
		return packer == VarLenPacker.STRING_ASCII;
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
	boolean checkSorted() {
		if (!isAscii())
			return super.checkSorted();
		else
			return checkSorted0(i -> element(i - 1).compareTo(element(i)));
	}

	@Override
	NonNullStringColumn toSorted0() {
		if (!isAscii())
			return super.toSorted0();
		else
			return toSorted00(this, (l, r) -> element(l + offset).compareTo(element(r + offset)));
	}
}
