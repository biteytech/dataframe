/*
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

import java.nio.charset.Charset;
import java.util.stream.Collector;

public interface StringColumn extends Column<String> {

	static final Charset UTF_8 = Charset.forName("UTF-8");

	@Override
	StringColumn subColumn(int fromIndex, int toIndex);

	@Override
	StringColumn subColumn(String fromElement, boolean fromInclusive, String toElement, boolean toInclusive);

	@Override
	StringColumn subColumn(String fromElement, String toElement);

	@Override
	StringColumn head(String toElement, boolean inclusive);

	@Override
	StringColumn head(String toElement);

	@Override
	StringColumn tail(String fromElement, boolean inclusive);

	@Override
	StringColumn tail(String fromElement);

	@Override
	StringColumn toHeap();

	@Override
	StringColumn toSorted();

	@Override
	StringColumn toDistinct();

	@Override
	StringColumn append(Column<String> tail);

	@Override
	StringColumn copy();

	/**
	 * Returns an {@link StringColumnBuilder builder} with the specified
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
	 * @return a new {@link StringColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static StringColumnBuilder builder(int characteristic) {
		return new StringColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link StringColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link StringColumnBuilder}
	 */
	public static StringColumnBuilder builder() {
		return new StringColumnBuilder(0);
	}

	/**
	 * Returns a new {@code StringColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code StringColumn} containing the specified elements.
	 */
	public static StringColumn of(String... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Strings} into a new {@code StringColumn} with the
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
	 * @return a new {@link StringColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<String, ?, StringColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), StringColumnBuilder::add, StringColumnBuilder::append,
				StringColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code Strings} into a new {@code StringColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link StringColumn}
	 */
	public static Collector<String, ?, StringColumn> collector() {
		return collector(0);
	}
}
