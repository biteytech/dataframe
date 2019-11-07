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

import java.nio.IntBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

abstract class NullableIntArrayColumn<E, I extends Column<E>, C extends IntArrayColumn<E, I, C>, N extends NullableColumn<E, I, C, N>> extends NullableColumn<E, I, C, N> {
	
	NullableIntArrayColumn(C column, BufferBitSet nonNulls, IntBuffer nullCounts, int offset, int size) {
		super(column, nonNulls, nullCounts, offset, size);
	}

	@Override
	void intersectRightSorted(C rhs, IntColumnBuilder indices, BufferBitSet keepLeft) {
		
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
