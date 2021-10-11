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
import java.time.Instant;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link Instant}.
 * <p>
 * Each element is stored as 12 bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface InstantColumn extends Column<Instant> {

	@Override
	InstantColumn subColumn(int fromIndex, int toIndex);

	@Override
	InstantColumn subColumnByValue(Instant fromElement, boolean fromInclusive, Instant toElement, boolean toInclusive);

	@Override
	InstantColumn subColumnByValue(Instant fromElement, Instant toElement);

	@Override
	InstantColumn head(Instant toElement, boolean inclusive);

	@Override
	InstantColumn head(Instant toElement);

	@Override
	InstantColumn tail(Instant fromElement, boolean inclusive);

	@Override
	InstantColumn tail(Instant fromElement);

	@Override
	InstantColumn toHeap();

	@Override
	InstantColumn toSorted();

	@Override
	InstantColumn toDistinct();

	@Override
	InstantColumn append(Column<Instant> tail);

	@Override
	InstantColumn copy();

	@Override
	InstantColumn clean(Predicate<Instant> predicate);

	/**
	 * Returns a {@link InstantColumnBuilder builder} with the specified
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
	 * @return a new {@link InstantColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static InstantColumnBuilder builder(int characteristic) {
		return new InstantColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link InstantColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link InstantColumnBuilder}
	 */
	public static InstantColumnBuilder builder() {
		return new InstantColumnBuilder(0);
	}

	/**
	 * Returns a new {@code InstantColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code InstantColumn} containing the specified elements.
	 */
	public static InstantColumn of(Instant... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Instant} into a new {@code InstantColumn} with
	 * the specified characteristic.
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
	 * @return a new {@link InstantColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<Instant, ?, InstantColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), InstantColumnBuilder::add, InstantColumnBuilder::append,
				InstantColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Instant} into a new {@code InstantColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link InstantColumn}
	 */
	public static Collector<Instant, ?, InstantColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code InstantColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code InstantColumn} containing the specified elements.
	 */
	public static InstantColumn of(Collection<Instant> c) {
		return c.stream().collect(collector());
	}
}
