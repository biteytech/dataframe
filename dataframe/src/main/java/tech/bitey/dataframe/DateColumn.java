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

import java.time.LocalDate;
import java.util.stream.Collector;

public interface DateColumn extends Column<LocalDate> {

	@Override
	DateColumn subColumn(int fromIndex, int toIndex);

	@Override
	DateColumn subColumn(LocalDate fromElement, boolean fromInclusive, LocalDate toElement, boolean toInclusive);

	@Override
	DateColumn subColumn(LocalDate fromElement, LocalDate toElement);

	@Override
	DateColumn head(LocalDate toElement, boolean inclusive);

	@Override
	DateColumn head(LocalDate toElement);

	@Override
	DateColumn tail(LocalDate fromElement, boolean inclusive);

	@Override
	DateColumn tail(LocalDate fromElement);

	@Override
	DateColumn toHeap();

	@Override
	DateColumn toSorted();

	@Override
	DateColumn toDistinct();

	@Override
	DateColumn append(Column<LocalDate> tail);

	@Override
	DateColumn copy();

	int yyyymmdd(int index);

	/**
	 * Returns a {@link DateColumnBuilder builder} with the specified
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
	 * @return a new {@link DateColumnBuilder}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static DateColumnBuilder builder(int characteristic) {
		return new DateColumnBuilder(characteristic);
	}

	/**
	 * Returns a new {@link DateColumnBuilder}
	 * <p>
	 * Equivalent to {@link #builder(int) builder(0)}
	 * 
	 * @return a new {@link DateColumnBuilder}
	 */
	public static DateColumnBuilder builder() {
		return new DateColumnBuilder(0);
	}

	/**
	 * Returns a new {@code DateColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code DateColumn} containing the specified elements.
	 */
	public static DateColumn of(LocalDate... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code LocalDates} into a new {@code DateColumn} with
	 * the specified characteristic.
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
	 * @return a new {@link DateColumn}
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	public static Collector<LocalDate, ?, DateColumn> collector(int characteristic) {
		return Collector.of(() -> builder(characteristic), DateColumnBuilder::add, DateColumnBuilder::append,
				DateColumnBuilder::build);
	}

	/**
	 * Collects a stream of {@code LocalDates} into a new {@code DateColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link DateColumn}
	 */
	public static Collector<LocalDate, ?, DateColumn> collector() {
		return collector(0);
	}
}
