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
import tech.bitey.bufferstuff.SmallDoubleBuffer;

/**
 * A builder for creating {@link DoubleColumn} instances. Example:
 *
 * <pre>
 * DoubleColumn column = DoubleColumn.builder().add(10d).addAll(200d, 300d, 400d).build();
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
public final class DoubleColumnBuilder
		extends SingleBufferColumnBuilder<Double, SmallDoubleBuffer, DoubleColumn, DoubleColumnBuilder> {

	DoubleColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(Double element) {
		add(element.doubleValue());
	}

	/**
	 * Adds a single {@code double} to the column.
	 *
	 * @param element the {@code double} to add
	 * 
	 * @return this builder
	 */
	public DoubleColumnBuilder add(double element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}

	/**
	 * Adds a sequence of {@code doubles} to the column.
	 *
	 * @param elements the {@code double} to add
	 * 
	 * @return this builder
	 */
	public DoubleColumnBuilder addAll(double... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	DoubleColumn emptyNonNull() {
		return NonNullDoubleColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
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
	DoubleColumn wrapNullableColumn(DoubleColumn column, BufferBitSet nonNulls) {
		return new NullableDoubleColumn((NonNullDoubleColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<Double> getType() {
		return ColumnType.DOUBLE;
	}

	@Override
	DoubleColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullDoubleColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	SmallDoubleBuffer asBuffer(BigByteBuffer buffer) {
		return buffer.asDoubleBuffer();
	}

	@Override
	int elementSize() {
		return 8;
	}

	@Override
	void append00(SmallDoubleBuffer elements) {
		SmallDoubleBuffer tail = elements.duplicate();
		tail.flip();
		this.elements.put(tail);
	}
}
