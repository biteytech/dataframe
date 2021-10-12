/*
 * Copyright 2021 biteytech@protonmail.com
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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link Short}.
 * <p>
 * Each element is stored as 2 bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface ShortColumn extends Column<Short> {

	@Override
	ShortColumn subColumn(int fromIndex, int toIndex);

	@Override
	ShortColumn subColumnByValue(Short fromElement, boolean fromInclusive, Short toElement, boolean toInclusive);

	@Override
	ShortColumn subColumnByValue(Short fromElement, Short toElement);

	@Override
	ShortColumn head(Short toElement, boolean inclusive);

	@Override
	ShortColumn head(Short toElement);

	@Override
	ShortColumn tail(Short fromElement, boolean inclusive);

	@Override
	ShortColumn tail(Short fromElement);

	@Override
	ShortColumn toHeap();

	@Override
	ShortColumn toSorted();

	@Override
	ShortColumn toDistinct();

	@Override
	ShortColumn append(Column<Short> tail);

	@Override
	ShortColumn copy();

	@Override
	ShortColumn clean(Predicate<Short> predicate);

	@Override
	ShortColumn filter(Predicate<Short> predicate, boolean keepNulls);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}.
	 * {@code null} values are not passed to the predicate for testing and are kept
	 * as-is. Equivalent to {@link #filter(Predicate, boolean) filter(predicate,
	 * true)}.
	 * 
	 * @param predicate the {@link Predicate} used to test for values which should
	 *                  be kept.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	default ShortColumn filter(Predicate<Short> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Primitive specialization of {@link Column#get(int)}.
	 * 
	 * @param index - index of the value to return
	 * 
	 * @return the short value at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	short getShort(int index);

	/**
	 * Returns a {@link ShortColumnBuilder builder} with the specified
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
	 * @return a new {@link ShortColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static ShortColumnBuilder builder(int characteristic) {
		return new ShortColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link ShortColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link ShortColumnBuilder}
	 */
	public static ShortColumnBuilder builder() {
		return new ShortColumnBuilder(0);
	}

	/**
	 * Returns a new {@code ShortColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code ShortColumn} containing the specified elements.
	 */
	public static ShortColumn of(Short... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Shorts} into a new {@code ShortColumn} with the
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
	 * @return a new {@link ShortColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<Short, ?, ShortColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), ShortColumnBuilder::add, ShortColumnBuilder::append,
				ShortColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Shorts} into a new {@code ShortColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link ShortColumn}
	 */
	public static Collector<Short, ?, ShortColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code ShortColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code ShortColumn} containing the specified elements.
	 */
	public static ShortColumn of(Collection<Short> c) {
		return c.stream().collect(collector());
	}
}
