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
import java.time.LocalTime;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link LocalTime}.
 * <p>
 * Each element is stored encoded as a {@code long} in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface TimeColumn extends Column<LocalTime> {

	@Override
	TimeColumn subColumn(int fromIndex, int toIndex);

	@Override
	TimeColumn subColumnByValue(LocalTime fromElement, boolean fromInclusive, LocalTime toElement, boolean toInclusive);

	@Override
	TimeColumn subColumnByValue(LocalTime fromElement, LocalTime toElement);

	@Override
	TimeColumn head(LocalTime toElement, boolean inclusive);

	@Override
	TimeColumn head(LocalTime toElement);

	@Override
	TimeColumn tail(LocalTime fromElement, boolean inclusive);

	@Override
	TimeColumn tail(LocalTime fromElement);

	@Override
	TimeColumn toHeap();

	@Override
	TimeColumn toSorted();

	@Override
	TimeColumn toDistinct();

	@Override
	TimeColumn append(Column<LocalTime> tail);

	@Override
	TimeColumn copy();

	@Override
	TimeColumn clean(Predicate<LocalTime> predicate);

	@Override
	TimeColumn filter(Predicate<LocalTime> predicate, boolean keepNulls);

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
	default TimeColumn filter(Predicate<LocalTime> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a {@link TimeColumnBuilder builder} with the specified
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
	 * @return a new {@link TimeColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static TimeColumnBuilder builder(int characteristic) {
		return new TimeColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link TimeColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link TimeColumnBuilder}
	 */
	public static TimeColumnBuilder builder() {
		return new TimeColumnBuilder(0);
	}

	/**
	 * Returns a new {@code TimeColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code TimeColumn} containing the specified elements.
	 */
	public static TimeColumn of(LocalTime... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code LocalTimes} into a new {@code TimeColumn} with
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
	 * @return a new {@link TimeColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<LocalTime, ?, TimeColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), TimeColumnBuilder::add, TimeColumnBuilder::append,
				TimeColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code LocalTimes} into a new {@code TimeColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link TimeColumn}
	 */
	public static Collector<LocalTime, ?, TimeColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code TimeColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code TimeColumn} containing the specified elements.
	 */
	public static TimeColumn of(Collection<LocalTime> c) {
		return c.stream().collect(collector());
	}
}
