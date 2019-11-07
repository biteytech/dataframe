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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.dataframe.IntArrayPacker.LOCAL_DATE;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

final class NonNullDateColumn extends IntArrayColumn<LocalDate, DateColumn, NonNullDateColumn> implements DateColumn {
	
	static final Map<Integer, NonNullDateColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullDateColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED, c -> new NonNullDateColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT, c -> new NonNullDateColumn(EMPTY_BUFFER, 0, 0, c, false));
	}
	static NonNullDateColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}
	
	NonNullDateColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, LOCAL_DATE, offset, size, characteristics, view);
	}

	@Override
	NonNullDateColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullDateColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	NonNullDateColumn empty() {
		return EMPTY.get(characteristics);
	}
	
	@Override
	public Comparator<LocalDate> comparator() {
		return LocalDate::compareTo;
	}

	@Override
	public int yyyymmdd(int index) {
		checkElementIndex(index, size);
		int packed = at(index+offset);
		return (packed >>> 16)*10000 + ((packed & 0xFF00) >>> 8)*100 + (packed & 0xFF);
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.DATE;
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof LocalDate;
	}
}
