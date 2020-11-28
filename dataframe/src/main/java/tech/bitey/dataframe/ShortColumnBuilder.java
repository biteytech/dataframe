/*
 * Copyright 2020 biteytech@protonmail.com
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

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;

/**
 * A builder for creating {@link ShortColumn} instances. Example:
 *
 * <pre>
 * ShortColumn column = ShortColumn.builder().add((short) 10).addAll((short) 200, (short) 300, (short) 400).build();
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
public final class ShortColumnBuilder
		extends SingleBufferColumnBuilder<Short, ShortBuffer, ShortColumn, ShortColumnBuilder> {

	ShortColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(Short element) {
		add(element.shortValue());
	}

	/**
	 * Adds a single {@code short} to the column.
	 *
	 * @param element the {@code short} to add
	 * 
	 * @return this builder
	 */
	public ShortColumnBuilder add(short element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}

	/**
	 * Adds a sequence of {@code shorts} to the column.
	 *
	 * @param elements the {@code short} to add
	 * 
	 * @return this builder
	 */
	public ShortColumnBuilder addAll(short... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	ShortColumn emptyNonNull() {
		return NonNullShortColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	boolean checkSorted() {
		return BufferUtils.isSorted(elements, 0, elements.position());
	}

	@Override
	boolean checkDistinct() {
		return BufferUtils.isSortedAndDistinct(elements, 0, elements.position());
	}

	@Override
	ShortColumn wrapNullableColumn(ShortColumn column, BufferBitSet nonNulls) {
		return new NullableShortColumn((NonNullShortColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<Short> getType() {
		return ColumnType.SHORT;
	}

	@Override
	ShortColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullShortColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	ShortBuffer asBuffer(ByteBuffer buffer) {
		return buffer.asShortBuffer();
	}

	@Override
	int elementSize() {
		return 2;
	}
}
