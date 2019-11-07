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

public interface FloatColumn extends NumericColumn<Float> {

	@Override
	FloatColumn subColumn(int fromIndex, int toIndex);

	@Override
	FloatColumn subColumn(Float fromElement, boolean fromInclusive, Float toElement, boolean toInclusive);

	@Override
	FloatColumn subColumn(Float fromElement, Float toElement);

	@Override
	FloatColumn head(Float toElement, boolean inclusive);

	@Override
	FloatColumn head(Float toElement);

	@Override
	FloatColumn tail(Float fromElement, boolean inclusive);

	@Override
	FloatColumn tail(Float fromElement);

	@Override
	FloatColumn toHeap();

	@Override
	FloatColumn toSorted();

	@Override
	FloatColumn toDistinct();

	@Override
	FloatColumn append(Column<Float> tail);

	@Override
	FloatColumn copy();

	/**
	 * Primitive specialization of {@link Column#get(int)}.
	 * 
	 * @param index - index of the value to return
	 * 
	 * @return the float value at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	float getFloat(int index);

	/**
	 * Returns a {@link FloatColumnBuilder builder} with the specified
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
	 * @return a new {@link FloatColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static FloatColumnBuilder builder(int characteristic) {
		return new FloatColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link FloatColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link FloatColumnBuilder}
	 */
	public static FloatColumnBuilder builder() {
		return new FloatColumnBuilder(0);
	}

	/**
	 * Returns a new {@code FloatColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code FloatColumn} containing the specified elements.
	 */
	public static FloatColumn of(Float... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Floats} into a new {@code FloatColumn} with the
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
	 * @return a new {@link FloatColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<Float, ?, FloatColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), FloatColumnBuilder::add, FloatColumnBuilder::append,
				FloatColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Floats} into a new {@code FloatColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link FloatColumn}
	 */
	public static Collector<Float, ?, FloatColumn> collector() {
		return collector(0);
	}
}
