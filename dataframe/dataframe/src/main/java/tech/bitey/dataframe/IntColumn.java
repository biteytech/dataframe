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

import static java.util.Spliterator.NONNULL;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.IntStream;

/**
 * A {@link Column} with element type {@link Integer}.
 * <p>
 * Each element is stored as 4 bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface IntColumn extends Column<Integer> {

	@Override
	IntColumn subColumn(int fromIndex, int toIndex);

	@Override
	IntColumn subColumnByValue(Integer fromElement, boolean fromInclusive, Integer toElement, boolean toInclusive);

	@Override
	IntColumn subColumnByValue(Integer fromElement, Integer toElement);

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

	@Override
	IntColumn clean(Predicate<Integer> predicate);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * {@link IntPredicate} and replacing with {@code null} when the predicate
	 * returns {@code true}.
	 * 
	 * @param predicate the {@code IntPredicate} used to test for values which
	 *                  should be {@code null}
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         {@code IntPredicate} and replacing with {@code null} when the
	 *         predicate returns {@code true}.
	 */
	IntColumn cleanInt(IntPredicate predicate);

	@Override
	IntColumn filter(Predicate<Integer> predicate, boolean keepNulls);

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
	default IntColumn filter(Predicate<Integer> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}. The
	 * {@code keepNulls} parameter determines whether all nulls are kept as-is, or
	 * if all nulls are removed.
	 * 
	 * @param predicate the {@link IntPredicate} used to test for values which
	 *                  should be kept.
	 * @param keepNulls {@code true} means keep all {@code null} values as-is.
	 *                  {@code false} means drop all {@code null} values.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	IntColumn filterInt(IntPredicate predicate, boolean keepNulls);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}.
	 * Equivalent to {@link #filter(IntPredicate, boolean) filter(predicate, true)}.
	 * 
	 * @param predicate the {@link IntPredicate} used to test for values which
	 *                  should be kept.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	default IntColumn filterInt(IntPredicate predicate) {
		return filterInt(predicate, true);
	}

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
	 * Primitive specialization of {@link Column#stream()}.
	 * 
	 * @return a primitive stream of the non-null elements in this {@link Column}.
	 */
	IntStream intStream();

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

	/**
	 * Returns a new {@code IntColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code IntColumn} containing the specified elements.
	 */
	public static IntColumn of(Collection<Integer> c) {
		return c.stream().collect(collector());
	}

	/**
	 * Returns a new {@code IntColumn} containing the specified elements.
	 * 
	 * @param stream the elements to be included in the new column
	 * 
	 * @return a new {@code IntColumn} containing the specified elements.
	 */
	public static IntColumn of(IntStream stream) {
		return stream.collect(() -> builder(NONNULL), (b, v) -> b.add(v), (a, b) -> a.append(b)).build();
	}
}
