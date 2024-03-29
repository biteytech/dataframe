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
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link Byte}.
 * 
 * @author biteytech@protonmail.com
 */
public interface ByteColumn extends Column<Byte> {

	@Override
	ByteColumn subColumn(int fromIndex, int toIndex);

	@Override
	ByteColumn subColumnByValue(Byte fromElement, boolean fromInclusive, Byte toElement, boolean toInclusive);

	@Override
	ByteColumn subColumnByValue(Byte fromElement, Byte toElement);

	@Override
	ByteColumn head(Byte toElement, boolean inclusive);

	@Override
	ByteColumn head(Byte toElement);

	@Override
	ByteColumn tail(Byte fromElement, boolean inclusive);

	@Override
	ByteColumn tail(Byte fromElement);

	@Override
	ByteColumn toHeap();

	@Override
	ByteColumn toSorted();

	@Override
	ByteColumn toDistinct();

	@Override
	ByteColumn append(Column<Byte> tail);

	@Override
	ByteColumn copy();

	@Override
	ByteColumn clean(Predicate<Byte> predicate);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * {@link BytePredicate} and replacing with {@code null} when the predicate
	 * returns {@code true}.
	 * 
	 * @param predicate the {@code BytePredicate} used to test for values which
	 *                  should be {@code null}
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         {@code BytePredicate} and replacing with {@code null} when the
	 *         predicate returns {@code true}.
	 */
	ByteColumn cleanByte(BytePredicate predicate);

	@Override
	ByteColumn filter(Predicate<Byte> predicate, boolean keepNulls);

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
	default ByteColumn filter(Predicate<Byte> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}. The
	 * {@code keepNulls} parameter determines whether all nulls are kept as-is, or
	 * if all nulls are removed.
	 * 
	 * @param predicate the {@link BytePredicate} used to test for values which
	 *                  should be kept.
	 * @param keepNulls {@code true} means keep all {@code null} values as-is.
	 *                  {@code false} means drop all {@code null} values.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	ByteColumn filterByte(BytePredicate predicate, boolean keepNulls);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}.
	 * Equivalent to {@link #filter(BytePredicate, boolean) filter(predicate,
	 * true)}.
	 * 
	 * @param predicate the {@link BytePredicate} used to test for values which
	 *                  should be kept.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	default ByteColumn filterByte(BytePredicate predicate) {
		return filterByte(predicate, true);
	}

	/**
	 * Derives a new {@link ByteColumn} from this one by applying the specified
	 * {@link ByteUnaryOperator} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @param op a {@link ByteUnaryOperator}
	 * 
	 * @return {@code op(this)}
	 */
	ByteColumn evaluate(ByteUnaryOperator op);

	/**
	 * Primitive specialization of {@link Column#get(int)}.
	 * 
	 * @param index - index of the value to return
	 * 
	 * @return the byte value at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	byte getByte(int index);

	/**
	 * Returns a {@link ByteColumnBuilder builder} with the specified
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
	 * @return a new {@link ByteColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static ByteColumnBuilder builder(int characteristic) {
		return new ByteColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link ByteColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link ByteColumnBuilder}
	 */
	public static ByteColumnBuilder builder() {
		return new ByteColumnBuilder(0);
	}

	/**
	 * Returns a new {@code ByteColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code ByteColumn} containing the specified elements.
	 */
	public static ByteColumn of(Byte... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Returns a new {@code ByteColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code ByteColumn} containing the specified elements.
	 */
	public static ByteColumn of(byte[] elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Bytes} into a new {@code ByteColumn} with the
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
	 * @return a new {@link ByteColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<Byte, ?, ByteColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), ByteColumnBuilder::add, ByteColumnBuilder::append,
				ByteColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Bytes} into a new {@code ByteColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link ByteColumn}
	 */
	public static Collector<Byte, ?, ByteColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code ByteColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code ByteColumn} containing the specified elements.
	 */
	public static ByteColumn of(Collection<Byte> c) {
		return c.stream().collect(collector());
	}
}
