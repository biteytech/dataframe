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

import java.io.InputStream;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * A {@link Column} for storing arbitrary byte streams ("blobs") represented by
 * {@link InputStream InputStreams}.
 * 
 * @author biteytech@protonmail.com
 */
public interface BlobColumn extends Column<InputStream> {

	@Override
	BlobColumn subColumn(int fromIndex, int toIndex);

	@Override
	BlobColumn toHeap();

	@Override
	BlobColumn append(Column<InputStream> tail);

	@Override
	BlobColumn copy();

	@Override
	BlobColumn clean(Predicate<InputStream> predicate);

	@Override
	BlobColumn filter(Predicate<InputStream> predicate, boolean keepNulls);

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
	default BlobColumn filter(Predicate<InputStream> predicate) {
		return filter(predicate, true);
	}

	/**
	 * Returns a new {@link BlobColumnBuilder}
	 * 
	 * @return a new {@link BlobColumnBuilder}
	 */
	public static BlobColumnBuilder builder() {
		return new BlobColumnBuilder(0);
	}

	/**
	 * Returns a new {@code InputStreamColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code InputStreamColumn} containing the specified elements.
	 */
	public static BlobColumn of(InputStream... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code InputStreams} into a new
	 * {@code InputStreamColumn}.
	 * <p>
	 * Equivalent to {@link #collector(int) collector(0)}
	 * 
	 * @return a new {@link BlobColumn}
	 */
	public static Collector<InputStream, ?, BlobColumn> collector() {
		return Collector.of(() -> builder(), BlobColumnBuilder::add, BlobColumnBuilder::append,
				BlobColumnBuilder::build);
	}

	/**
	 * Returns a new {@code InputStreamColumn} containing the specified elements.
	 * 
	 * @param c the elements to be included in the new column
	 * 
	 * @return a new {@code InputStreamColumn} containing the specified elements.
	 */
	public static BlobColumn of(Collection<InputStream> c) {
		return c.stream().collect(collector());
	}
}
