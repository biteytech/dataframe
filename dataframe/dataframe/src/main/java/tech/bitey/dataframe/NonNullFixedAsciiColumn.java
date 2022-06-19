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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BIG_BUFFER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferUtils;

final class NonNullFixedAsciiColumn extends NonNullFixedLenColumn<String, FixedAsciiColumn, NonNullFixedAsciiColumn>
		implements FixedAsciiColumn {

	static final Map<Integer, NonNullFixedAsciiColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS,
				c -> new NonNullFixedAsciiColumn(0, EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullFixedAsciiColumn(0, EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullFixedAsciiColumn(0, EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullFixedAsciiColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	private final int width;

	NonNullFixedAsciiColumn(int width, BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);

		Pr.checkArgument(width >= 0, "width must be positive");
		this.width = width;
	}

	@Override
	NonNullFixedAsciiColumn construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullFixedAsciiColumn(width, buffer, offset, size, characteristics, view);
	}

	ByteBuffer at(int index) {
		final long from = (long) index * width;
		return buffer.smallSlice(from, from + width);
	}

	@Override
	String getNoOffset(int index) {
		return US_ASCII.decode(at(index)).toString();
	}

	@Override
	NonNullFixedAsciiColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.FSTRING;
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {

			result = 31 * result + at(i).hashCode();
		}
		return result;
	}

	@Override
	void put(BigByteBuffer buffer, int index) {
		buffer.put(at(index));
	}

	@Override
	int compareValuesAt(NonNullFixedAsciiColumn rhs, int l, int r) {
		return at(l).compareTo(rhs.at(r));
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof String;
	}

	@Override
	int elementSize() {
		return width;
	}

	private void put(int i, ByteBuffer bb) {
		buffer.put((long) i * width, bb, 0, width);
	}

	@Override
	void swap(int i, int j) {

		ByteBuffer temp = BufferUtils.copy(at(i), 0, width);
		put(i, at(j));
		put(j, temp);
	}

	@Override
	int deduplicate(int fromIndex, int toIndex) {

		if (toIndex - fromIndex < 2)
			return toIndex;

		ByteBuffer prev = at(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			ByteBuffer bb = at(i);

			if (!prev.equals(bb)) {
				if (highest < i)
					put(highest, bb);

				highest++;
				prev = bb;
			}
		}

		return highest;
	}

	@Override
	void writeTo0(WritableByteChannel channel, ByteOrder order) throws IOException {
		writeInt(channel, order, width);
	}

	@Override
	NonNullFixedAsciiColumn readFrom0(ReadableByteChannel channel, ByteOrder order, BigByteBuffer bbb, int size)
			throws IOException {

		int width = readInt(channel, null);

		return new NonNullFixedAsciiColumn(width, bbb, 0, size, characteristics, false);
	}
}
