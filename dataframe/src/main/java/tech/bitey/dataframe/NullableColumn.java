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

import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;
import static tech.bitey.dataframe.guava.DfPreconditions.checkPositionIndex;

import java.nio.IntBuffer;
import java.util.Comparator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;

abstract class NullableColumn<E, I extends Column<E>, C extends NonNullColumn<E, I, C>, N extends NullableColumn<E, I, C, N>> extends AbstractColumn<E, I, N> {

	final C column;
	final C subColumn;
	final BufferBitSet nonNulls;
	final IntBuffer nullCounts;
	
	NullableColumn(C column, BufferBitSet nonNulls, IntBuffer nullCounts, int offset, int size) {
		super(offset, size, nullCounts == null);
		
		if(column.view)
			column = column.slice();
		
		this.column = column;
		this.nonNulls = nonNulls;
		
		if(nullCounts == null) {
			final int words = (size - 1) / 32;
			this.nullCounts = BufferUtils.allocate(words * 4).asIntBuffer();
			for(int i = 0, w = 0, count = 0; w < words; w++) {
				for(int j = 0; i < size && j < 32; j++, i++)
					if(!nonNulls.get(i))
						count++;
				this.nullCounts.put(w, count);
			}
		}
		else
			this.nullCounts = nullCounts;
		
		final int firstNonNullIndex = firstNonNullIndex();
		if(firstNonNullIndex == -1)
			this.subColumn = column.empty();
		else
			this.subColumn = column.subColumn(firstNonNullIndex, lastNonNullIndex()+1);
	}
	
	@Override
	public int characteristics() {
		return BASE_CHARACTERISTICS;
	}

	@Override
	public N toHeap() {
		@SuppressWarnings("unchecked")
		N cast = (N)this;
		return cast;
	}

	@Override
	public N toSorted() {
		throw new UnsupportedOperationException("columns with null values cannot be sorted");
	}

	@Override
	public N toDistinct() {
		throw new UnsupportedOperationException("columns with null values cannot be sorted");
	}
	
	@Override
	E getNoOffset(int index) {
		if(nonNulls.get(index))
			return column.getNoOffset(nonNullIndex(index));
		else
			return null;
	}
	
	@Override
	boolean isNullNoOffset(int index) {
		return !nonNulls.get(index);
	}
	
	private int firstNonNullIndex() {
		int index = nonNulls.nextSetBit(offset);
		return index == -1 || index > lastIndex() ? -1 : nonNullIndex(index);
	}
	
	private int lastNonNullIndex() {
		int index = nonNulls.previousSetBit(lastIndex());
		return index < offset ? -1 : nonNullIndex(index);
	}
	
	int nonNullIndex(int index) {
		
		// count null bits before index		
		int word = (index - 1) / 32;
		int count = word == 0 ? 0 : nullCounts.get(word - 1);
		
		for(int i = word*32; i < index; i++)
			if(!nonNulls.get(i))
				count++;
		
		return index - count;
	}
	
	private int nullIndex(int index) {
		
		int nullIndex = -1;
		for(int i = 0; i <= index; i++)
			nullIndex = nonNulls.nextSetBit(nullIndex+1);
		
		return nullIndex;
	}
	
	void checkGetPrimitive(int index) {
		checkElementIndex(index, size);
		
		if(isNullNoOffset(index+offset))
			throw new NullPointerException();
	}
	
	private int indexOf0(Object o, boolean first) {
		if((o != null && !checkType(o)) || isEmpty())
			return -1;
		else if(o == null) {
			if(first) {
				int index = nonNulls.nextClearBit(offset);
				return index > lastIndex() ? -1 : index - offset;
			}
			else {
				int index = nonNulls.previousClearBit(lastIndex());
				return index < offset ? -1 : index - offset;
			}
		}
		
		@SuppressWarnings("unchecked")
		E value = (E)o;
		
		int index = subColumn.search(value, first);
		index = nullIndex(index);
		
		return index < offset || index > lastIndex() ? -1 : index - offset;
	}
	
	@Override
	public int indexOf(Object o) {
		return indexOf0(o, true);
	}
	
	@Override
	public int lastIndexOf(Object o) {
		return indexOf0(o, false);
	}
	
	@Override
	public boolean contains(Object o) {
		return indexOf(o) != -1;
	}
	
	@Override
	public ListIterator<E> listIterator(final int idx) {
		
		checkPositionIndex(idx, size);
		
		return new ImmutableListIterator<E>() {
			
			int index = idx + offset;
			
			@Override
			public boolean hasNext() {				
				return index <= lastIndex();
			}

			@Override
			public E next() {
				if(!hasNext())
					throw new NoSuchElementException("called next when hasNext is false");

				if(nonNulls.get(index))
					return column.get(nonNullIndex(index++));
				else {
					index++;
					return null;
				}
			}

			@Override
			public boolean hasPrevious() {
				return index > offset;
			}

			@Override
			public E previous() {
				if(!hasPrevious())
					throw new NoSuchElementException("called previous when hasPrevious is false");
				
				if(nonNulls.get(--index))
					return column.get(nonNullIndex(index));
				else {
					return null;
				}
			}

			@Override
			public int nextIndex() {
				return index-offset;
			}

			@Override
			public int previousIndex() {
				return index-offset-1;
			}
		};
	}


	@Override
	public Comparator<? super E> comparator() {
		return column.comparator();
	}
		
	@Override
	boolean equals0(@SuppressWarnings("rawtypes") NullableColumn rhs) {
		
		// check that values and nulls are in the same place
		int count = 0;
		for(int i = offset, j = rhs.offset; i <= lastIndex(); i++, j++) {
			if(nonNulls.get(i) != rhs.nonNulls.get(j))
				return false;
			count += nonNulls.get(i) ? 1 : 0;
		}
		
		if(count > 0) {
			@SuppressWarnings("unchecked")
			C cast = (C)rhs.column;			
			return column.equals0(cast, firstNonNullIndex(), rhs.firstNonNullIndex(), count);
		}
		else
			return true; // all nulls
	}
	
	@Override
	public int hashCode() {
		if(isEmpty()) return 1;
				
		// hash value positions
		int result = 1;
		for (int i = offset; i <= lastIndex(); i++)
			result = 31 * result + (nonNulls.get(i) ? 1231 : 1237);
		
		// hash actual values
		return 31 * result + subColumn.hashCode();
	}

	abstract N construct(C column, BufferBitSet nonNulls, int size);
	
	@Override
	Column<E> applyFilter0(BufferBitSet keep, int cardinality) {
		
		if(keep.equals(nonNulls))
			return column;
		
		BufferBitSet filteredNonNulls = new BufferBitSet();
		BufferBitSet keepNonNulls = new BufferBitSet();
		
		int nullCount = 0;
		for(int i = offset, j = 0; i <= lastIndex(); i++) {
			if(keep.get(i-offset)) {
				if(nonNulls.get(i)) {
					filteredNonNulls.set(j);
					keepNonNulls.set(nonNullIndex(i));
				}
				else
					nullCount++;
				j++;
			}
		}
		
		@SuppressWarnings("unchecked")
		C column = (C)this.column.applyFilter(keepNonNulls, keepNonNulls.cardinality());
		
		if(nullCount == 0)
			return column;
		else
			return construct(column, filteredNonNulls, column.size() + nullCount);
	}
	
	@Override
	Column<E> select0(IntColumn indices) {
		
		BufferBitSet decodedNonNulls = new BufferBitSet();
		int cardinality = 0;
		for(int i = 0; i < indices.size(); i++) {
			if(nonNulls.get(indices.getInt(i)+offset)) {
				decodedNonNulls.set(i);
				cardinality++;
			}
		}
		
		IntColumnBuilder nonNullIndices = IntColumn.builder().ensureCapacity(cardinality);
		for(int i = 0; i < indices.size(); i++) {
			int index = indices.getInt(i) + offset;
			if(nonNulls.get(index))
				nonNullIndices.add(nonNullIndex(index));
		}
		
		@SuppressWarnings("unchecked")
		C column = (C)this.column.select(nonNullIndices.build());
		
		if(cardinality == indices.size())
			return column;
		else
			return construct(column, decodedNonNulls, indices.size());
	}
		
	@SuppressWarnings("unchecked")
	I prependNonNull(C head) {
		
		BufferBitSet nonNulls = this.nonNulls.get(offset, offset+this.size());
		nonNulls = nonNulls.shiftRight(head.size());
		nonNulls.set(0, head.size());
		
		final C column;
		if(this.subColumn.isEmpty())
			column = head;
		else
			column = head.appendNonNull(this.subColumn);
		
		return (I)construct(column, nonNulls, head.size() + this.size());
	}
	
	@SuppressWarnings("unchecked")
	@Override
	I append0(Column<E> tail) {
		
		final int size = this.size() + tail.size();
		
		if(!tail.isNonnull()) {			
			// append nullable column
			N rhs = (N)tail;
			
			C column = (C)this.subColumn.append(rhs.subColumn);
			
			BufferBitSet nonNulls = this.nonNulls.get(offset, offset+this.size());
			BufferBitSet bothNonNulls = rhs.nonNulls.get(rhs.offset, rhs.offset+rhs.size());
			
			bothNonNulls = bothNonNulls.shiftRight(size());
			bothNonNulls.or(nonNulls);
			
			return (I)construct(column, bothNonNulls, size);
		}
		else {
			// append non-null column
			C rhs = (C)tail;
			
			C column = (C)this.subColumn.append(rhs);
			
			BufferBitSet nonNulls = this.nonNulls.get(offset, offset+this.size());
			nonNulls.set(this.size(), size);
			
			return (I)construct(column, nonNulls, size);
		}
	}
	
	@Override
	public N copy() {		
		@SuppressWarnings("unchecked")
		C column = (C)subColumn.copy();
		
		return construct(column, nonNulls.get(offset, offset+size), size);
	}
	
	@Override
	int intersectBothSorted(N rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectBothSorted");
	}
	
	@Override
	IntColumn intersectLeftSorted(I rhs, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectLeftSorted");
	}
	
	abstract void intersectRightSorted(C rhs, IntColumnBuilder indices, BufferBitSet keepLeft);
	
	// does not implement navigableset methods

	@Override
	public E lower(E e) {
		throw new UnsupportedOperationException("lower");
	}

	@Override
	public E floor(E e) {
		throw new UnsupportedOperationException("floor");
	}

	@Override
	public E ceiling(E e) {
		throw new UnsupportedOperationException("ceiling");
	}

	@Override
	public E higher(E e) {
		throw new UnsupportedOperationException("higher");
	}	
	
	// does not implement subColumn-by-element methods
	
	@Override
	public N subColumn(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		throw new UnsupportedOperationException("subColumn");
	}
	
	@Override
	public N subColumn(E fromElement, E toElement) {
		throw new UnsupportedOperationException("subColumn");
	}
	
	@Override
	public N head(E toElement, boolean inclusive) {
		throw new UnsupportedOperationException("head");
	}
	
	@Override
	public N head(E toElement) {
		throw new UnsupportedOperationException("head");
	}
	
	@Override
	public N tail(E fromElement, boolean inclusive) {
		throw new UnsupportedOperationException("tail");
	}
	
	@Override
	public N tail(E fromElement) {
		throw new UnsupportedOperationException("tail");
	}
	
	@Override
	public ColumnType getType() {
		return column.getType();
	}
}
