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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link UUID}.
 * <p>
 * Each element is stored as 16 bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface UuidColumn extends Column<UUID> {

	@Override
	UuidColumn subColumn(int fromIndex, int toIndex);

	@Override
	UuidColumn subColumnByValue(UUID fromElement, boolean fromInclusive, UUID toElement, boolean toInclusive);

	@Override
	UuidColumn subColumnByValue(UUID fromElement, UUID toElement);

	@Override
	UuidColumn head(UUID toElement, boolean inclusive);

	@Override
	UuidColumn head(UUID toElement);

	@Override
	UuidColumn tail(UUID fromElement, boolean inclusive);

	@Override
	UuidColumn tail(UUID fromElement);

	@Override
	UuidColumn toHeap();

	@Override
	UuidColumn toSorted();

	@Override
	UuidColumn toDistinct();

	@Override
	UuidColumn append(Column<UUID> tail);

	@Override
	UuidColumn copy();

	@Override
	UuidColumn clean(Predicate<UUID> predicate);

	@Override
	UuidColumn filter(Predicate<UUID> predicate, boolean keepNulls);

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
	default UuidColumn filter(Predicate<UUID> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a {@link UuidColumnBuilder builder} with the specified
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
	 * @return a new {@link UuidColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static UuidColumnBuilder builder(int characteristic) {
		return new UuidColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link UuidColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link UuidColumnBuilder}
	 */
	public static UuidColumnBuilder builder() {
		return new UuidColumnBuilder(0);
	}

	/**
	 * Returns a new {@code UuidColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code UuidColumn} containing the specified elements.
	 */
	public static UuidColumn of(UUID... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code UUID} into a new {@code UuidColumn} with the
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
	 * @return a new {@link UuidColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<UUID, ?, UuidColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), UuidColumnBuilder::add, UuidColumnBuilder::append,
				UuidColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code UUID} into a new {@code UuidColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link UuidColumn}
	 */
	public static Collector<UUID, ?, UuidColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code UuidColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code UuidColumn} containing the specified elements.
	 */
	public static UuidColumn of(Collection<UUID> c) {
		return c.stream().collect(collector());
	}
}
