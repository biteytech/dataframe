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
import java.util.stream.LongStream;

/**
 * A {@link Column} with element type {@link Long}.
 * <p>
 * Each element is stored as 8 bytes in a {@link ByteBuffer}.
 * 
 * @author biteytech@protonmail.com
 */
public interface LongColumn extends Column<Long> {

	@Override
	LongColumn subColumn(int fromIndex, int toIndex);

	@Override
	LongColumn subColumnByValue(Long fromElement, boolean fromInclusive, Long toElement, boolean toInclusive);

	@Override
	LongColumn subColumnByValue(Long fromElement, Long toElement);

	@Override
	LongColumn head(Long toElement, boolean inclusive);

	@Override
	LongColumn head(Long toElement);

	@Override
	LongColumn tail(Long fromElement, boolean inclusive);

	@Override
	LongColumn tail(Long fromElement);

	@Override
	LongColumn toHeap();

	@Override
	LongColumn toSorted();

	@Override
	LongColumn toDistinct();

	@Override
	LongColumn append(Column<Long> tail);

	@Override
	LongColumn copy();

	/**
	 * Primitive specialization of {@link Column#get(int)}.
	 * 
	 * @param index - index of the value to return
	 * 
	 * @return the long value at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	long getLong(int index);

	/**
	 * Primitive specialization of {@link Column#stream()}.
	 * 
	 * @return a primitive stream of the non-null elements in this {@link Column}.
	 */
	LongStream longStream();

	/**
	 * Returns a {@link LongColumnBuilder builder} with the specified
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
	 * @return a new {@link LongColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static LongColumnBuilder builder(int characteristic) {
		return new LongColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link LongColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link LongColumnBuilder}
	 */
	public static LongColumnBuilder builder() {
		return new LongColumnBuilder(0);
	}

	/**
	 * Returns a new {@code LongColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code LongColumn} containing the specified elements.
	 */
	public static LongColumn of(Long... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Longs} into a new {@code LongColumn} with the
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
	 * @return a new {@link LongColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<Long, ?, LongColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), LongColumnBuilder::add, LongColumnBuilder::append,
				LongColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Longs} into a new {@code LongColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link LongColumn}
	 */
	public static Collector<Long, ?, LongColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code LongColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code LongColumn} containing the specified elements.
	 */
	public static LongColumn of(Collection<Long> c) {
		return c.stream().collect(collector());
	}

	/**
	 * Returns a new {@code LongColumn} containing the specified elements.
	 * 
	 * @param stream the elements to be included in the new column
	 * 
	 * @return a new {@code LongColumn} containing the specified elements.
	 */
	public static LongColumn of(LongStream stream) {
		return stream.collect(() -> builder(NONNULL), (b, v) -> b.add(v), (a, b) -> a.append(b)).build();
	}
}
