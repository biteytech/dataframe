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

/**
 * Utility methods for searching {@link AbstractColumn}
 * 
 * @author biteytech@protonmail.com
 */
enum AbstractColumnSearch {
	;

	static <E extends Comparable<? super E>> int search(AbstractColumn<E, ?, ?> col, E value, boolean first) {

		final int offset = col.offset;

		if (col.isSorted()) {
			int index = AbstractColumnSearch.binarySearch(col, offset, offset + col.size, value);
			if (col.isDistinct() || index < 0)
				return index;
			else if (first)
				return AbstractColumnSearch.binaryFindFirst(col, index, value);
			else
				return AbstractColumnSearch.binaryFindLast(col, index, value);
		} else
			return col.indexOf(value, first) + offset;
	}

	static <E extends Comparable<? super E>> int binaryFindFirst(AbstractColumn<E, ?, ?> col, int fromIndex, E key) {

		final int offset = col.offset;

		while (offset != fromIndex && col.getNoOffset(fromIndex - 1).equals(key)) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex - range;
			} while (offset <= rangeIndex && col.getNoOffset(rangeIndex).equals(key));

			fromIndex -= range >> 1;
		}

		return fromIndex;
	}

	static <E extends Comparable<? super E>> int binaryFindLast(AbstractColumn<E, ?, ?> col, int fromIndex, E key) {

		final int lastIndex = col.lastIndex();

		while (fromIndex != lastIndex && col.getNoOffset(fromIndex + 1).equals(key)) {

			int range = 1, rangeIndex;
			do {
				range <<= 1;
				rangeIndex = fromIndex + range;
			} while (rangeIndex <= lastIndex && col.getNoOffset(rangeIndex).equals(key));

			fromIndex += range >> 1;
		}

		return fromIndex;
	}

	/**
	 * Searches for the specified value using the binary search algorithm.
	 * <p>
	 * Adapted from {@code Arrays::binarySearch0}
	 *
	 * @param col the column to search
	 * @param key the value to be searched for
	 * @return index of the specified key, if it is contained in this column;
	 *         otherwise, {@code (-(<i>insertion point</i>) - 1)}. The <i>insertion
	 *         point</i> is defined as the point at which the key would be inserted
	 *         into the array: the index of the first element greater than the key,
	 *         or {@code a.length} if all elements in the array are less than the
	 *         specified key. Note that this guarantees that the return value will
	 *         be &gt;= 0 if and only if the key is found.
	 */
	static <E extends Comparable<? super E>> int binarySearch(AbstractColumn<E, ?, ?> col, int fromIndex, int toIndex,
			E key) {

		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;

			E midVal = col.getNoOffset(mid);

			int cmp = midVal.compareTo(key);

			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}
}
