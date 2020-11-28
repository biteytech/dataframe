/*
 * Copyright 2020 biteytech@protonmail.com
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

import tech.bitey.bufferstuff.BufferBitSet;

@FunctionalInterface
interface INullCounts {

	/*
	 * Could be any size. A larger word size means fewer counts (so less extra
	 * memory used to achieve constant-time lookups), but more expensive calls to
	 * nonNulls.cardinality (larger ranges). A size of 32 works out to one bit per
	 * index for large Columns.
	 */
	static final int WORD_SHIFT = 5;
	static final int WORD_SIZE = 1 << WORD_SHIFT;

	int nonNullIndex(int index);

	static INullCounts of(BufferBitSet nonNulls, int size) {

		if (size <= WORD_SIZE)
			return i -> nonNulls.cardinality(0, i);
		else
			return new NullCounts(nonNulls, size);
	}
}
