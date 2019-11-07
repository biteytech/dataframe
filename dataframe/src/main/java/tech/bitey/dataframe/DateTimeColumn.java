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

import java.time.LocalDateTime;
import java.util.stream.Collector;

public interface DateTimeColumn extends Column<LocalDateTime> {

	@Override
	DateTimeColumn subColumn(int fromIndex, int toIndex);

	@Override
	DateTimeColumn subColumn(LocalDateTime fromElement, boolean fromInclusive, LocalDateTime toElement,
			boolean toInclusive);

	@Override
	DateTimeColumn subColumn(LocalDateTime fromElement, LocalDateTime toElement);

	@Override
	DateTimeColumn head(LocalDateTime toElement, boolean inclusive);

	@Override
	DateTimeColumn head(LocalDateTime toElement);

	@Override
	DateTimeColumn tail(LocalDateTime fromElement, boolean inclusive);

	@Override
	DateTimeColumn tail(LocalDateTime fromElement);

	@Override
	DateTimeColumn toHeap();

	@Override
	DateTimeColumn toSorted();

	@Override
	DateTimeColumn toDistinct();

	@Override
	DateTimeColumn append(Column<LocalDateTime> tail);

	@Override
	DateTimeColumn copy();

	/**
	 * Returns a {@link DateTimeColumnBuilder builder} with the specified
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
	 * @return a new {@link DateTimeColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static DateTimeColumnBuilder builder(int characteristic) {
		return new DateTimeColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link DateTimeColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link DateTimeColumnBuilder}
	 */
	public static DateTimeColumnBuilder builder() {
		return new DateTimeColumnBuilder(0);
	}

	/**
	 * Returns a new {@code DateTimeColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code DateTimeColumn} containing the specified elements.
	 */
	public static DateTimeColumn of(LocalDateTime... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code LocalDateTimes} into a new {@code DateTimeColumn}
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
	 * @return a new {@link DateTimeColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<LocalDateTime, ?, DateTimeColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), DateTimeColumnBuilder::add, DateTimeColumnBuilder::append,
				DateTimeColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code LocalDateTimes} into a new
	 * {@code DateTimeColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link DateTimeColumn}
	 */
	public static Collector<LocalDateTime, ?, DateTimeColumn> collector() {
		return collector(0);
	}
}
