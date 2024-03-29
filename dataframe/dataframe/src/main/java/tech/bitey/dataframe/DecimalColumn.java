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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link BigDecimal}.
 * <p>
 * Elements are stored in a {@link ByteBuffer} as a scale ({@code int}) and an
 * unscaled {@link BigInteger} ({@link BigInteger#toByteArray()}), where the
 * value is {@code (unscaled x 10^-scale)}
 * 
 * @author biteytech@protonmail.com
 */
public interface DecimalColumn extends Column<BigDecimal> {

	@Override
	DecimalColumn subColumn(int fromIndex, int toIndex);

	@Override
	DecimalColumn subColumnByValue(BigDecimal fromElement, boolean fromInclusive, BigDecimal toElement,
			boolean toInclusive);

	@Override
	DecimalColumn subColumnByValue(BigDecimal fromElement, BigDecimal toElement);

	@Override
	DecimalColumn head(BigDecimal toElement, boolean inclusive);

	@Override
	DecimalColumn head(BigDecimal toElement);

	@Override
	DecimalColumn tail(BigDecimal fromElement, boolean inclusive);

	@Override
	DecimalColumn tail(BigDecimal fromElement);

	@Override
	DecimalColumn toHeap();

	@Override
	DecimalColumn toSorted();

	@Override
	DecimalColumn toDistinct();

	@Override
	DecimalColumn append(Column<BigDecimal> tail);

	@Override
	DecimalColumn copy();

	@Override
	DecimalColumn clean(Predicate<BigDecimal> predicate);

	@Override
	DecimalColumn filter(Predicate<BigDecimal> predicate, boolean keepNulls);

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
	default DecimalColumn filter(Predicate<BigDecimal> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a {@link DecimalColumnBuilder builder} with the specified
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
	 * @return a new {@link DecimalColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static DecimalColumnBuilder builder(int characteristic) {
		return new DecimalColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link DecimalColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link DecimalColumnBuilder}
	 */
	public static DecimalColumnBuilder builder() {
		return new DecimalColumnBuilder(0);
	}

	/**
	 * Returns a new {@code BigDecimalColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code BigDecimalColumn} containing the specified elements.
	 */
	public static DecimalColumn of(BigDecimal... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code BigDecimals} into a new {@code BigDecimalColumn}
	 * with the specified characteristic.
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
	 * @return a new {@link DecimalColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<BigDecimal, ?, DecimalColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), DecimalColumnBuilder::add, DecimalColumnBuilder::append,
				DecimalColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code BigDecimals} into a new {@code BigDecimalColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link DecimalColumn}
	 */
	public static Collector<BigDecimal, ?, DecimalColumn> collector() {
		return collector(0);
	}

	/**
	 * Returns a new {@code DecimalColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code DecimalColumn} containing the specified elements.
	 */
	public static DecimalColumn of(Collection<BigDecimal> c) {
		return c.stream().collect(collector());
	}
}
