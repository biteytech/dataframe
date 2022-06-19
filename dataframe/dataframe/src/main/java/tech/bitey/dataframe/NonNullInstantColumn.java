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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BigByteBuffer;

final class NonNullInstantColumn extends NonNullFixedLenColumn<Instant, InstantColumn, NonNullInstantColumn>
		implements InstantColumn {

	static final Map<Integer, NonNullInstantColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullInstantColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullInstantColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullInstantColumn(EMPTY_BIG_BUFFER, 0, 0, c, false));
	}

	static NonNullInstantColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	NonNullInstantColumn(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullInstantColumn construct(BigByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullInstantColumn(buffer, offset, size, characteristics, view);
	}

	private long second(int index) {
		return buffer.getLong((long) index * 12);
	}

	private int nano(int index) {
		return buffer.getInt((long) index * 12 + 8);
	}

	private void put(int index, long seconds, int nanos) {

		final long lindex = (long) index * 12;

		buffer.putLong(lindex, seconds);
		buffer.putInt(lindex + 8, nanos);
	}

	@Override
	Instant getNoOffset(int index) {
		return Instant.ofEpochSecond(second(index), nano(index));
	}

	@Override
	NonNullInstantColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	public ColumnType<Instant> getType() {
		return ColumnType.INSTANT;
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {

			long second = second(i);
			int nano = nano(i);
			long hilo = second ^ nano;

			result = 31 * result + ((int) (hilo >> 32)) ^ (int) hilo;
		}
		return result;
	}

	@Override
	void put(BigByteBuffer buffer, int index) {
		buffer.putLong(second(index));
		buffer.putInt(nano(index));
	}

	@Override
	int compareValuesAt(NonNullInstantColumn rhs, int l, int r) {

		return (this.second(l) < rhs.second(r) ? -1
				: (this.second(l) > rhs.second(r) ? 1
						: (this.nano(l) < rhs.nano(r) ? -1 : (this.nano(l) > rhs.nano(r) ? 1 : 0))));
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Instant;
	}

	@Override
	int elementSize() {
		return 12;
	}

	@Override
	void swap(int i, int j) {
		long is = second(i);
		int in = nano(i);
		long js = second(j);
		int jn = nano(j);

		put(i, js, jn);
		put(j, is, in);
	}

	@Override
	int deduplicate(int fromIndex, int toIndex) {

		if (toIndex - fromIndex < 2)
			return toIndex;

		long prevS = second(fromIndex);
		int prevN = nano(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			long second = second(i);
			int nano = nano(i);

			if (prevS != second || prevN != nano) {
				if (highest < i)
					put(highest, second, nano);

				highest++;
				prevS = second;
				prevN = nano;
			}
		}

		return highest;
	}
}
