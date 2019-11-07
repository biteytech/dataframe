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

import static java.lang.Integer.compare;
import static java.util.Spliterator.NONNULL;
import static tech.bitey.bufferstuff.BufferSort.inplaceSort;
import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferSearch;
import tech.bitey.bufferstuff.BufferUtils;

abstract class IntArrayColumn<E, I extends Column<E>, C extends IntArrayColumn<E, I, C>> extends NonNullSingleBufferColumn<E, I, C> {

	final IntArrayPacker<E> packer;
	final IntBuffer elements;
	
	IntArrayColumn(ByteBuffer buffer, IntArrayPacker<E> packer, int offset, int size, int characteristics, boolean view) {
		super(buffer, offset, size, characteristics, view);
		
		this.packer = packer;
		this.elements = buffer.asIntBuffer();
	}
	
	int at(int index) {
		return elements.get(index);
	}
	
	@Override
	E getNoOffset(int index) {
		return packer.unpack(at(index));
	}
	
	int search(int packed) {
		return BufferSearch.binarySearch(elements, offset, offset+size, packed);		
	}
	
	@Override
	int search(E value, boolean first) {
		
		final int packed = packer.pack(value);
		
		if(isSorted()) {
			int index = search(packed);
			if(isDistinct() || index < 0)
				return index;			
			else if(first)
				return BufferSearch.binaryFindFirst(elements, offset, index);
			else
				return BufferSearch.binaryFindLast(elements, offset+size, index);
		}
		else {
			if(first) {
				for(int i = offset; i <= lastIndex(); i++)
					if(at(i) == packed)
						return i;
			}
			else {
				for(int i = lastIndex(); i >= offset; i--)
					if(at(i) == packed)
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
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++)
			result = 31 * result + at(i);
		return result;
	}
	
	@Override
	C applyFilter0(BufferBitSet keep, int cardinality) {
		
		ByteBuffer buffer = allocate(cardinality);
		for(int i = offset; i <= lastIndex(); i++)
			if(keep.get(i - offset))
				buffer.putInt(at(i));
		buffer.flip();
		
		return construct(buffer, 0, cardinality, characteristics, false);
	}

	@Override
	C select0(IntColumn indices) {
		
		ByteBuffer buffer = allocate(indices.size());
		for(int i = 0; i < indices.size(); i++)
			buffer.putInt(at(indices.getInt(i)+offset));
		buffer.flip();
		
		return construct(buffer, 0, indices.size(), NONNULL, false);
	}

	@Override
	int compareValuesAt(C rhs, int l, int r) {
		return compare(at(l + offset), rhs.at(r + rhs.offset));
	}

	@Override
	void intersectLeftSorted(C rhs, IntColumnBuilder indices, BufferBitSet keepRight) {
		
		for(int i = rhs.offset; i <= rhs.lastIndex(); i++) {
			
			int leftIndex = search(rhs.at(i));			
			if(leftIndex >= offset && leftIndex <= lastIndex()) {
				
				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	int elementSize() {
		return 4;
	}
}
