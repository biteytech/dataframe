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
 * A builder for creating {@link LongColumn} instances. Example:
 *
 * <pre>
 * LongColumn column = LongColumn.builder().add(10L).addAll(200L, 300L, 400L).build();
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
public final class LongColumnBuilder extends LongArrayColumnBuilder<Long, LongColumn, LongColumnBuilder> {

	LongColumnBuilder(int characteristics) {
		super(characteristics, LongArrayPacker.LONG);
	}

	@Override
	LongColumn emptyNonNull() {
		return NonNullLongColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	LongColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullLongColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	LongColumn wrapNullableColumn(LongColumn column, BufferBitSet nonNulls) {
		return new NullableLongColumn((NonNullLongColumn) column, nonNulls, null, 0, size);
	}

	/**
	 * Adds a single {@code long} to the column.
	 *
	 * @param element the {@code long} to add
	 * 
	 * @return this builder
	 */
	public LongColumnBuilder add(long element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}

	/**
	 * Adds a sequence of {@code longs} to the column.
	 *
	 * @param elements the {@code longs} to add
	 * 
	 * @return this builder
	 */
	public LongColumnBuilder addAll(long... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	public ColumnType<Long> getType() {
		return ColumnType.LONG;
	}
}
