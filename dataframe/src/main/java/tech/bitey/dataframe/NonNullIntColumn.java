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
import static tech.bitey.dataframe.IntArrayPacker.INTEGER;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

final class NonNullIntColumn extends IntArrayColumn<Integer, IntColumn, NonNullIntColumn> implements IntColumn {

	static final Map<Integer, NonNullIntColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullIntColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED, c -> new NonNullIntColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT, c -> new NonNullIntColumn(EMPTY_BUFFER, 0, 0, c, false));
	}
	static NonNullIntColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}
	
	NonNullIntColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, INTEGER, offset, size, characteristics, view);
	}

	@Override
	NonNullIntColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullIntColumn(buffer, offset, size, characteristics, view);
	}

	@Override
	public double mean() {
		if(size == 0)
			return Double.NaN;
		
		long sum = 0;
		for(int i = 0; i < size; i++)
			sum += at(i+offset);
		return sum / (double)size;
	}

	@Override
	NonNullIntColumn empty() {
		return EMPTY.get(characteristics);
	}
	
	@Override
	public Comparator<Integer> comparator() {
		return Integer::compareTo;
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.INT;
	}

	@Override
	public int getInt(int index) {
		checkElementIndex(index, size);
		return at(index+offset);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Integer;
	}
}
