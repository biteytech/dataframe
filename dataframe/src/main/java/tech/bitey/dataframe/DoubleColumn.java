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
	 * @param elements the elements to be included in the new column
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
