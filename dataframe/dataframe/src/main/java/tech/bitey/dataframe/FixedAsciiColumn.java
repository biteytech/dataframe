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

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link String}.
 * <p>
 * The strings must only contain ASCII characters, and are stored with a fixed
 * width. All strings must have the same length.
 * 
 * @author biteytech@protonmail.com
 */
public interface FixedAsciiColumn extends StringColumn {

	@Override
	FixedAsciiColumn subColumn(int fromIndex, int toIndex);

	@Override
	FixedAsciiColumn subColumnByValue(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive);

	@Override
	FixedAsciiColumn subColumnByValue(String fromElement, String toElement);

	@Override
	FixedAsciiColumn head(String toElement, boolean inclusive);

	@Override
	FixedAsciiColumn head(String toElement);

	@Override
	FixedAsciiColumn tail(String fromElement, boolean inclusive);

	@Override
	FixedAsciiColumn tail(String fromElement);

	@Override
	FixedAsciiColumn toHeap();

	@Override
	FixedAsciiColumn toSorted();

	@Override
	FixedAsciiColumn toDistinct();

	@Override
	FixedAsciiColumn append(Column<String> tail);

	@Override
	FixedAsciiColumn copy();

	@Override
	FixedAsciiColumn clean(Predicate<String> predicate);

	@Override
	FixedAsciiColumn filter(Predicate<String> predicate, boolean keepNulls);

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
	@Override
	default FixedAsciiColumn filter(Predicate<String> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns an {@link FixedAsciiColumnBuilder builder} with the specified
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
	 * @return a new {@link FixedAsciiColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static FixedAsciiColumnBuilder builder(int characteristic) {
		return new FixedAsciiColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link FixedAsciiColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link FixedAsciiColumnBuilder}
	 */
	public static FixedAsciiColumnBuilder builder() {
		return builder(0);
	}

	/**
	 * Returns a new {@code FixedAsciiColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code FixedAsciiColumn} containing the specified elements.
	 */
	public static FixedAsciiColumn of(String... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Strings} into a new {@code FixedAsciiColumn}.
	 * 
	 * @return a new {@link FixedAsciiColumn}
	 */
	public static Collector<String, ?, FixedAsciiColumn> collector() {
		return Collector.of(FixedAsciiColumn::builder, FixedAsciiColumnBuilder::add, FixedAsciiColumnBuilder::append,
				FixedAsciiColumnBuilder::build);
	}

	/**
	 * Returns a new {@code FixedAsciiColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code FixedAsciiColumn} containing the specified elements.
	 */
	public static FixedAsciiColumn of(Collection<String> c) {
		return c.stream().collect(collector());
	}

	@Override
	default StringColumn toStringColumn() {
		return toStringColumn(Function.identity());
	}
}
