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

import java.util.Spliterator;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link IntColumn} instances. Example:
 *
 * <pre>
 * IntColumn column = IntColumn.builder().add(10).addAll(200, 300, 400).build();
 * </pre>
 * 
 * Elements appear in the resulting column in the same order they were added to
 * the builder.
 * <p>
 * Builder instances can be reused; it is safe to call
 * {@link ColumnBuilder#build build} multiple times to build multiple columns in
 * series. Each new column contains all the elements of the ones created before
 * it.
 *
 * @author biteytech@protonmail.com
 */
public final class IntColumnBuilder extends IntArrayColumnBuilder<Integer, IntColumn, IntColumnBuilder> {

	IntColumnBuilder(int characteristics) {
		super(characteristics, IntArrayPacker.INTEGER);
	}

	@Override
	IntColumn emptyNonNull() {
		return NonNullIntColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	IntColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullIntColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	IntColumn wrapNullableColumn(IntColumn column, BufferBitSet nonNulls) {
		return new NullableIntColumn((NonNullIntColumn) column, nonNulls, null, 0, size);
	}

	/**
	 * Adds a single {@code int} to the column.
	 *
	 * @param element the {@code int} to add
	 * 
	 * @return this builder
	 */
	public IntColumnBuilder add(int element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}

	/**
	 * Adds a sequence of {@code ints} to the column.
	 *
	 * @param elements the {@code ints} to add
	 * 
	 * @return this builder
	 */
	public IntColumnBuilder addAll(int... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	/**
	 * Returns {@link ColumnType#INT}
	 * 
	 * @return {@code ColumnType.INT}
	 */
	@Override
	public ColumnType<Integer> getType() {
		return ColumnType.INT;
	}
}
