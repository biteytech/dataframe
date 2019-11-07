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
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferSort.inplaceSort;
import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;
import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BUFFER;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSearch;
import tech.bitey.bufferstuff.BufferUtils;

final class NonNullFloatColumn extends NonNullSingleBufferColumn<Float, FloatColumn, NonNullFloatColumn> implements FloatColumn {
	
	static final Map<Integer, NonNullFloatColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS, c -> new NonNullFloatColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED, c -> new NonNullFloatColumn(EMPTY_BUFFER, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT, c -> new NonNullFloatColumn(EMPTY_BUFFER, 0, 0, c, false));
	}
	static NonNullFloatColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}
	
	private final FloatBuffer elements;
	
	NonNullFloatColumn(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);
		
		this.elements = buffer.asFloatBuffer();
	}
	
	@Override
	NonNullFloatColumn construct(ByteBuffer buffer, int offset, int size, int characteristics, boolean view) {
		return new NonNullFloatColumn(buffer, offset, size, characteristics, view);
	}
	
	float at(int index) {
		return elements.get(index);
	}
	
	@Override
	Float getNoOffset(int index) {
		return at(index);
	}

	@Override
	public double mean() {
		if(size == 0)
        	return Double.NaN;
		
		double sum = 0;
		for(int i = offset; i < size; i++)
			sum += at(i);

        // Compute initial estimate using definitional formula
        double xbar = sum / size;

        // Compute correction factor in second pass
        double correction = 0;
        for (int i = offset; i < size; i++) {
            correction += at(i) - xbar;
        }
        return xbar + (correction/size);
	}

	int search(float value) {
		return BufferSearch.binarySearch(elements, offset, offset+size, value);
	}
	
	@Override
	int search(Float value, boolean first) {
		if(isSorted()) {
			int index = search(value);
			if(isDistinct() || index < 0)
				return index;			
			else if(first)
				return BufferSearch.binaryFindFirst(elements, offset, index);
			else
				return BufferSearch.binaryFindLast(elements, offset+size, index);
		}
		else {
			float d = value;
			
			if(first) {
				for(int i = offset; i <= lastIndex(); i++)
					if(Float.compare(at(i), d) == 0)
						return i;
			}
			else {
				for(int i = lastIndex(); i >= offset; i--)
					if(Float.compare(at(i), d) == 0)
						return i;
			}
			
			return -1;
		}
	}
	
	@Override
	void sort() {
		inplaceSort(elements, offset, offset+size);
	}
	
	@Override
	int deduplicate() {
		return BufferUtils.deduplicate(elements, offset, offset+size);
	}

	@Override
	boolean checkSorted() {
		return BufferUtils.isSorted(elements, offset, offset+size);
	}

	@Override
	boolean checkDistinct() {
		return isSortedAndDistinct(elements, offset, offset+size);
	}

	@Override
	NonNullFloatColumn empty() {
		return EMPTY.get(characteristics);
	}
	
	@Override
	public Comparator<Float> comparator() {
		return Float::compareTo;
	}
	
	@Override
	public float getFloat(int index) {
		checkElementIndex(index, size);
		return at(index+offset);
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.FLOAT;
	}
	
	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++) {
			int bits = Float.floatToIntBits(at(i));
			result = 31 * result + bits;
		}
		return result;
	}

	@Override
	NonNullFloatColumn applyFilter0(BufferBitSet keep, int cardinality) {
		
		ByteBuffer buffer = allocate(cardinality);
		for(int i = offset; i <= lastIndex(); i++)
			if(keep.get(i - offset))
				buffer.putFloat(at(i));
		buffer.flip();
		
		return new NonNullFloatColumn(buffer, 0, cardinality, characteristics, false);
	}

	@Override
	NonNullFloatColumn select0(IntColumn indices) {
		
		ByteBuffer buffer = allocate(indices.size());
		for(int i = 0; i < indices.size(); i++)
			buffer.putFloat(at(indices.getInt(i)+offset));
		buffer.flip();
		
		return construct(buffer, 0, indices.size(), NONNULL, false);
	}

	@Override
	int compareValuesAt(NonNullFloatColumn rhs, int l, int r) {
		return Float.compare(at(l + offset), rhs.at(r + rhs.offset));
	}

	@Override
	void intersectLeftSorted(NonNullFloatColumn rhs, IntColumnBuilder indices, BufferBitSet keepRight) {
		
		for(int i = rhs.offset; i <= rhs.lastIndex(); i++) {
			
			int leftIndex = search(rhs.at(i));			
			if(leftIndex >= offset && leftIndex <= lastIndex()) {
				
				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Float;
	}

	@Override
	int elementSize() {
		return 4;
	}
}
