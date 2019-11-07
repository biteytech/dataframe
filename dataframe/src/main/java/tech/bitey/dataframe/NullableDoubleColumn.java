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

import static tech.bitey.bufferstuff.BufferBitSet.EMPTY_BITSET;
import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.nio.IntBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableDoubleColumn extends NullableColumn<Double, DoubleColumn, NonNullDoubleColumn, NullableDoubleColumn> implements DoubleColumn {
	
	static final NullableDoubleColumn EMPTY = new NullableDoubleColumn(NonNullDoubleColumn.EMPTY.get(NONNULL_CHARACTERISTICS), EMPTY_BITSET, null, 0, 0);

	NullableDoubleColumn(NonNullDoubleColumn column, BufferBitSet nonNulls, IntBuffer nullCounts, int offset, int size) {
		super(column, nonNulls, nullCounts, offset, size);
	}

	@Override
	NullableDoubleColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableDoubleColumn(column, nonNulls, nullCounts, fromIndex+offset, toIndex-fromIndex);
	}

	@Override
	NullableDoubleColumn empty() {
		return EMPTY;
	}

	@Override
	public double mean() {
		return subColumn.mean();
	}

	@Override
	public double getDouble(int index) {
		checkGetPrimitive(index);
		return column.getDouble(nonNullIndex(index+offset));
	}

	@Override
	NullableDoubleColumn construct(NonNullDoubleColumn column, BufferBitSet nonNulls, int size) {
		return new NullableDoubleColumn(column, nonNulls, null, 0, size);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof Double;
	}

	@Override
	void intersectRightSorted(NonNullDoubleColumn rhs, IntColumnBuilder indices, BufferBitSet keepLeft) {
		
		for(int i = offset; i <= lastIndex(); i++) {
			
			if(!nonNulls.get(i))
				continue;
			
			int rightIndex = rhs.search(column.at(nonNullIndex(i)));			
			if(rightIndex >= rhs.offset && rightIndex <= rhs.lastIndex()) {
				
				indices.add(rightIndex - rhs.offset);
				keepLeft.set(i - offset);
			}
		}
	}
}
