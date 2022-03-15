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

import static java.util.Spliterator.NONNULL;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;

/**
 * A {@link Column} with element type {@link Double}.
 * <p>
 * Each element is stored as 8 bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface DoubleColumn extends Column<Double> {

	@Override
	DoubleColumn subColumn(int fromIndex, int toIndex);

	@Override
	DoubleColumn subColumnByValue(Double fromElement, boolean fromInclusive, Double toElement, boolean toInclusive);

	@Override
	DoubleColumn subColumnByValue(Double fromElement, Double toElement);

	@Override
	DoubleColumn head(Double toElement, boolean inclusive);

	@Override
	DoubleColumn head(Double toElement);

	@Override
	DoubleColumn tail(Double fromElement, boolean inclusive);

	@Override
	DoubleColumn tail(Double fromElement);

	@Override
	DoubleColumn toHeap();

	@Override
	DoubleColumn toSorted();

	@Override
	DoubleColumn toDistinct();

	@Override
	DoubleColumn append(Column<Double> tail);

	@Override
	DoubleColumn copy();

	@Override
	DoubleColumn clean(Predicate<Double> predicate);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * {@link DoublePredicate} and replacing with {@code null} when the predicate
	 * returns {@code true}.
	 * 
	 * @param predicate the {@code DoublePredicate} used to test for values which
	 *                  should be {@code null}
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         {@code DoublePredicate} and replacing with {@code null} when the
	 *         predicate returns {@code true}.
	 */
	DoubleColumn cleanDouble(DoublePredicate predicate);

	/**
	 * Returns a new column derived by replacing each occurrence of {@code NaN} with
	 * {@code null}.
	 * 
	 * @return a new column derived by replacing each occurrence of {@code NaN} with
	 *         {@code null}.
	 */
	default DoubleColumn cleanNaN() {
		return cleanDouble(Double::isNaN);
	}

	@Override
	DoubleColumn filter(Predicate<Double> predicate, boolean keepNulls);

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
	default DoubleColumn filter(Predicate<Double> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}. The
	 * {@code keepNulls} parameter determines whether all nulls are kept as-is, or
	 * if all nulls are removed.
	 * 
	 * @param predicate the {@link DoublePredicate} used to test for values which
	 *                  should be kept.
	 * @param keepNulls {@code true} means keep all {@code null} values as-is.
	 *                  {@code false} means drop all {@code null} values.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	DoubleColumn filterDouble(DoublePredicate predicate, boolean keepNulls);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}.
	 * Equivalent to {@link #filter(DoublePredicate, boolean) filter(predicate,
	 * true)}.
	 * 
	 * @param predicate the {@link DoublePredicate} used to test for values which
	 *                  should be kept.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	default DoubleColumn filterDouble(DoublePredicate predicate) {
		return filterDouble(predicate, true);
	}

	/**
	 * Derives a new {@link DoubleColumn} from this one by applying the specified
	 * {@link DoubleUnaryOperator} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @param op a {@link DoubleUnaryOperator}
	 * 
	 * @return {@code op(this)}
	 */
	DoubleColumn evaluate(DoubleUnaryOperator op);

	/**
	 * Primitive specialization of {@link Column#get(int)}.
	 * 
	 * @param index - index of the value to return
	 * 
	 * @return the double value at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	double getDouble(int index);

	/**
	 * Primitive specialization of {@link Column#stream()}.
	 * 
	 * @return a primitive stream of the non-null elements in this {@link Column}.
	 */
	DoubleStream doubleStream();

	/**
	 * Returns a {@link DoubleColumnBuilder builder} with the specified
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
	 * @return a new {@link DoubleColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static DoubleColumnBuilder builder(int characteristic) {
		return new DoubleColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link DoubleColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link DoubleColumnBuilder}
	 */
	public static DoubleColumnBuilder builder() {
		return new DoubleColumnBuilder(0);
	}

	/**
	 * Returns a new {@code DoubleColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code DoubleColumn} containing the specified elements.
	 */
	public static DoubleColumn of(Double... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Returns a new {@code DoubleColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code DoubleColumn} containing the specified elements.
	 */
	public static DoubleColumn of(double[] elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Doubles} into a new {@code DoubleColumn} with the
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
	 * @return a new {@link DoubleColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<Double, ?, DoubleColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), DoubleColumnBuilder::add, DoubleColumnBuilder::append,
				DoubleColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Doubles} into a new {@code DoubleColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link DoubleColumn}
	 */
	public static Collector<Double, ?, DoubleColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code DoubleColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code DoubleColumn} containing the specified elements.
	 */
	public static DoubleColumn of(Collection<Double> c) {
		return c.stream().collect(collector());
	}

	/**
	 * Returns a new {@code DoubleColumn} containing the specified elements.
	 * 
	 * @param stream the elements to be included in the new column
	 * 
	 * @return a new {@code DoubleColumn} containing the specified elements.
	 */
	public static DoubleColumn of(DoubleStream stream) {
		return stream.collect(() -> builder(NONNULL), (b, v) -> b.add(v), (a, b) -> a.append(b)).build();
	}
}
