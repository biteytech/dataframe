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
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link Float}.
 * <p>
 * Each element is stored as 4 bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface FloatColumn extends Column<Float> {

	@Override
	FloatColumn subColumn(int fromIndex, int toIndex);

	@Override
	FloatColumn subColumnByValue(Float fromElement, boolean fromInclusive, Float toElement, boolean toInclusive);

	@Override
	FloatColumn subColumnByValue(Float fromElement, Float toElement);

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

	@Override
	FloatColumn clean(Predicate<Float> predicate);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * {@link FloatPredicate} and replacing with {@code null} when the predicate
	 * returns {@code true}.
	 * 
	 * @param predicate the {@code FloatPredicate} used to test for values which
	 *                  should be {@code null}
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         {@code FloatPredicate} and replacing with {@code null} when the
	 *         predicate returns {@code true}.
	 */
	FloatColumn cleanFloat(FloatPredicate predicate);

	@Override
	FloatColumn filter(Predicate<Float> predicate, boolean keepNulls);

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
	default FloatColumn filter(Predicate<Float> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}. The
	 * {@code keepNulls} parameter determines whether all nulls are kept as-is, or
	 * if all nulls are removed.
	 * 
	 * @param predicate the {@link FloatPredicate} used to test for values which
	 *                  should be kept.
	 * @param keepNulls {@code true} means keep all {@code null} values as-is.
	 *                  {@code false} means drop all {@code null} values.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	FloatColumn filterFloat(FloatPredicate predicate, boolean keepNulls);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}.
	 * Equivalent to {@link #filter(FloatPredicate, boolean) filter(predicate,
	 * true)}.
	 * 
	 * @param predicate the {@link FloatPredicate} used to test for values which
	 *                  should be kept.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	default FloatColumn filterFloat(FloatPredicate predicate) {
		return filterFloat(predicate, true);
	}

	/**
	 * Returns a new column derived by replacing each occurrence of {@code NaN} with
	 * {@code null}.
	 * 
	 * @return a new column derived by replacing each occurrence of {@code NaN} with
	 *         {@code null}.
	 */
	default FloatColumn cleanNaN() {
		return cleanFloat(Float::isNaN);
	}

	/**
	 * Derives a new {@link FloatColumn} from this one by applying the specified
	 * {@link FloatUnaryOperator} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @param op a {@link FloatUnaryOperator}
	 * 
	 * @return {@code op(this)}
	 */
	FloatColumn evaluate(FloatUnaryOperator op);

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
	 * Returns a new {@code FloatColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code FloatColumn} containing the specified elements.
	 */
	public static FloatColumn of(float[] elements) {
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

	/**
	 * Returns a new {@code FloatColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code FloatColumn} containing the specified elements.
	 */
	public static FloatColumn of(Collection<Float> c) {
		return c.stream().collect(collector());
	}
}
