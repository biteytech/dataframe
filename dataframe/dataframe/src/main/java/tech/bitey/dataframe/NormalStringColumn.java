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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} with element type {@link String}.
 * <p>
 * Elements are normalized by assigning a {@code byte} value to each distinct
 * element. So there can only be up to 256 distinct elements per column.
 * <p>
 * Does not support index operations (e.g. {@link #toSorted()},
 * {@link #toDistinct()}, etc.)
 * 
 * @author biteytech@protonmail.com
 */
public interface NormalStringColumn extends StringColumn {

	@Override
	NormalStringColumn subColumn(int fromIndex, int toIndex);

	@Override
	NormalStringColumn subColumnByValue(String fromElement, boolean fromInclusive, String toElement,
			boolean toInclusive);

	@Override
	NormalStringColumn subColumnByValue(String fromElement, String toElement);

	@Override
	NormalStringColumn head(String toElement, boolean inclusive);

	@Override
	NormalStringColumn head(String toElement);

	@Override
	NormalStringColumn tail(String fromElement, boolean inclusive);

	@Override
	NormalStringColumn tail(String fromElement);

	@Override
	NormalStringColumn toHeap();

	@Override
	NormalStringColumn toSorted();

	@Override
	NormalStringColumn toDistinct();

	@Override
	NormalStringColumn append(Column<String> tail);

	@Override
	NormalStringColumn copy();

	@Override
	NormalStringColumn clean(Predicate<String> predicate);

	@Override
	NormalStringColumn filter(Predicate<String> predicate, boolean keepNulls);

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
	@Override
	default NormalStringColumn filter(Predicate<String> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a new {@link NormalStringColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link NormalStringColumnBuilder}
	 */
	public static NormalStringColumnBuilder builder() {
		return new NormalStringColumnBuilder();
	}

	/**
	 * Returns a new {@code NormalStringColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code NormalStringColumn} containing the specified elements.
	 */
	public static NormalStringColumn of(String... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Strings} into a new {@code NormalStringColumn}.
	 * 
	 * @return a new {@link NormalStringColumn}
	 */
	public static Collector<String, ?, NormalStringColumn> collector() {
		return Collector.of(NormalStringColumn::builder, NormalStringColumnBuilder::add,
				NormalStringColumnBuilder::append, NormalStringColumnBuilder::build);
	}

	/**
	 * Returns a new {@code NormalStringColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code NormalStringColumn} containing the specified elements.
	 */
	public static NormalStringColumn of(Collection<String> c) {
		return c.stream().collect(collector());
	}

	@Override
	default StringColumn toStringColumn() {
		return toStringColumn(Function.identity());
	}

	@Override
	default NormalStringColumn normalize(double threshold) {
		return this;
	}
}
