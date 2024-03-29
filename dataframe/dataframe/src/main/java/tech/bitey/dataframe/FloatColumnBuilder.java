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
import tech.bitey.bufferstuff.BufferUtils;
import tech.bitey.bufferstuff.SmallFloatBuffer;

/**
 * A builder for creating {@link FloatColumn} instances. Example:
 *
 * <pre>
 * FloatColumn column = FloatColumn.builder().add(10f).addAll(200f, 300f, 400f).build();
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
public final class FloatColumnBuilder
		extends SingleBufferColumnBuilder<Float, SmallFloatBuffer, FloatColumn, FloatColumnBuilder> {

	FloatColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(Float element) {
		add(element.floatValue());
	}

	/**
	 * Adds a single {@code float} to the column.
	 *
	 * @param element the {@code float} to add
	 * 
	 * @return this builder
	 */
	public FloatColumnBuilder add(float element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}

	/**
	 * Adds a sequence of {@code floats} to the column.
	 *
	 * @param elements the {@code float} to add
	 * 
	 * @return this builder
	 */
	public FloatColumnBuilder addAll(float... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	FloatColumn emptyNonNull() {
		return NonNullFloatColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
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
	FloatColumn wrapNullableColumn(FloatColumn column, BufferBitSet nonNulls) {
		return new NullableFloatColumn((NonNullFloatColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<Float> getType() {
		return ColumnType.FLOAT;
	}

	@Override
	FloatColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullFloatColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	SmallFloatBuffer asBuffer(BigByteBuffer buffer) {
		return buffer.asFloatBuffer();
	}

	@Override
	int elementSize() {
		return 4;
	}

	@Override
	void append00(SmallFloatBuffer elements) {
		SmallFloatBuffer tail = elements.duplicate();
		tail.flip();
		this.elements.put(tail);
	}
}
