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

import java.util.stream.Collector;

public interface IntColumn extends NumericColumn<Integer> {

	@Override
	IntColumn subColumn(int fromIndex, int toIndex);

	@Override
	IntColumn subColumn(Integer fromElement, boolean fromInclusive, Integer toElement, boolean toInclusive);

	@Override
	IntColumn subColumn(Integer fromElement, Integer toElement);

	@Override
	IntColumn head(Integer toElement, boolean inclusive);

	@Override
	IntColumn head(Integer toElement);

	@Override
	IntColumn tail(Integer fromElement, boolean inclusive);

	@Override
	IntColumn tail(Integer fromElement);

	@Override
	IntColumn toHeap();

	@Override
	IntColumn toSorted();

	@Override
	IntColumn toDistinct();

	@Override
	IntColumn append(Column<Integer> tail);

	@Override
	IntColumn copy();

	/**
	 * Primitive specialization of {@link Column#get(int)}.
	 * 
	 * @param index - index of the value to return
	 * 
	 * @return the int value at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	int getInt(int index);

	/**
	 * Returns an {@link IntColumnBuilder builder} with the specified
	 * characteristic.
	 * 
	 * @param characteristic - one of:
	 *                       <ul>
	 *                       <li>{@code 0} (zero) - no constraints on the elements
	 *                       to be added to the column
	 *                       <li>{@link java.util.Spliterator#NONNULL NONNULL}
	 *                       <li>{@link java.util.Spliterator#SORTED SORTED}
	 *                       <li>{@link java.util.Spliterator#DISTINCT DISTINCT}
	 *                       </ul>
	 * 
	 * @return a new {@link IntColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static IntColumnBuilder builder(int characteristic) {
		return new IntColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link IntColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link IntColumnBuilder}
	 */
	public static IntColumnBuilder builder() {
		return new IntColumnBuilder(0);
	}

	/**
	 * Returns a new {@code IntColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code IntColumn} containing the specified elements.
	 */
	public static IntColumn of(Integer... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Integers} into a new {@code IntColumn} with the
	 * specified characteristic.
	 * 
	 * @param characteristic - one of:
	 *                       <ul>
	 *                       <li>{@code 0} (zero) - no constraints on the elements
	 *                       to be added to the column
	 *                       <li>{@link java.util.Spliterator#NONNULL NONNULL}
	 *                       <li>{@link java.util.Spliterator#SORTED SORTED}
	 *                       <li>{@link java.util.Spliterator#DISTINCT DISTINCT}
	 *                       </ul>
	 * 
	 * @return a new {@link IntColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<Integer, ?, IntColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), IntColumnBuilder::add, IntColumnBuilder::append,
				IntColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Integers} into a new {@code IntColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link IntColumn}
	 */
	public static Collector<Integer, ?, IntColumn> collector() {
		return collector(0);
	}
}
