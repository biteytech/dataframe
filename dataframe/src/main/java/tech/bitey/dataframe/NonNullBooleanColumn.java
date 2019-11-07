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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Spliterator.NONNULL;
import static tech.bitey.bufferstuff.BufferBitSet.EMPTY_BITSET;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.util.Comparator;

import tech.bitey.bufferstuff.BufferBitSet;

final class NonNullBooleanColumn extends NonNullColumn<Boolean, BooleanColumn, NonNullBooleanColumn> implements BooleanColumn {

	static final NonNullBooleanColumn EMPTY = new NonNullBooleanColumn(EMPTY_BITSET, 0, 0, false);
	
	private final BufferBitSet elements;
	
	NonNullBooleanColumn(BufferBitSet elements, int offset, int size, boolean view) {
		super(offset, size, NONNULL, view);
		this.elements = elements;
	}

	@Override
	NonNullBooleanColumn withCharacteristics(int characteristics) {
		throw new IllegalStateException();
	}
	
	@Override
	Boolean getNoOffset(int index) {
		return elements.get(index) ? TRUE : FALSE;
	}

	@Override
	NonNullBooleanColumn subColumn0(int fromIndex, int toIndex) {
		return new NonNullBooleanColumn(elements, fromIndex+offset, toIndex-fromIndex, true);
	}

	@Override
	int search(Boolean value, boolean first) {
		if(isSorted())
			throw new IllegalStateException();
		else if(first) {
			if(value)
				return elements.nextSetBit(offset);
			else {
				// special case - all bits are considered clear after last index
				int index = elements.nextClearBit(offset);
				return index > lastIndex() ? -1 : index;
			}
		}
		else {
			if(value)
				return elements.previousSetBit(lastIndex());
			else
				return elements.previousClearBit(lastIndex());
		}
	}

	@Override
	NonNullBooleanColumn empty() {
		return EMPTY;
	}

	@Override
	public Comparator<Boolean> comparator() {
		return Boolean::compareTo;
	}

	@Override
	public boolean getBoolean(int index) {
		checkElementIndex(index, size);
		return elements.get(index+offset);
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.BOOLEAN;
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++)
			result = 31 * result + (elements.get(i) ? 1231 : 1237);
		return result;
	}

	@Override
	boolean equals0(NonNullBooleanColumn rhs, int lStart, int rStart, int length) {
		for(int i = 0; i < length; i++)
			if(elements.get(lStart+i) != rhs.elements.get(rStart+i))
				return false;
		return true;
	}

	@Override
	NonNullBooleanColumn applyFilter0(BufferBitSet keep, int cardinality) {
		
		BufferBitSet elements = new BufferBitSet();
		for(int i = offset, j = 0; i <= lastIndex(); i++) {
			if(keep.get(i - offset)) {
				if(this.elements.get(i))
					elements.set(j);
				j++;
			}
		}
		
		return new NonNullBooleanColumn(elements, 0, cardinality, false);
	}

	@Override
	NonNullBooleanColumn select0(IntColumn indices) {
		
		BufferBitSet elements = new BufferBitSet();
		for(int i = 0; i < indices.size(); i++)
			if(this.elements.get(indices.getInt(i) + offset))
				elements.set(i);
		
		return new NonNullBooleanColumn(elements, 0, indices.size(), false);
	}

	@Override
	NonNullBooleanColumn appendNonNull(NonNullBooleanColumn tail) {
		
		BufferBitSet elements = tail.elements.get(tail.offset, tail.offset+tail.size).shiftRight(size());
		elements.or(this.elements.get(offset, offset+size));
			
		return new NonNullBooleanColumn(elements, 0, this.size() + tail.size(), false);
	}

	@Override
	int intersectBothSorted(NonNullBooleanColumn rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectBothSorted");
	}

	@Override
	void intersectLeftSorted(NonNullBooleanColumn rhs, IntColumnBuilder indices, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectLeftSorted");	
	}

	@Override
	int compareValuesAt(NonNullBooleanColumn rhs, int l, int r) {
		throw new UnsupportedOperationException("compareValuesAt");
	}

	@Override
	NonNullBooleanColumn toSorted0() {
		throw new UnsupportedOperationException("toSorted");
	}

	@Override
	NonNullBooleanColumn toDistinct0(NonNullBooleanColumn sorted) {
		throw new UnsupportedOperationException("toDistinct0");
	}

	@Override
	boolean checkSorted() {
		throw new UnsupportedOperationException("checkSorted");
	}

	@Override
	boolean checkDistinct() {
		throw new UnsupportedOperationException("checkDistinct");
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Boolean;
	}

	@Override
	public NonNullBooleanColumn copy() {		
		return new NonNullBooleanColumn(elements.get(offset, offset+size), 0, size, false);
	}

	@Override
	public NonNullBooleanColumn slice() {
		return copy();
	}
}
