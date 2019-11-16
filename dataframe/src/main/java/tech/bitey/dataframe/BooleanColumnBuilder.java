/*
 * Copyright 2019 biteytech@protonmail.com
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

import static tech.bitey.dataframe.NonNullBooleanColumn.EMPTY;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link BooleanColumn} instances. Example:
 *
 * <pre>
 * BooleanColumn column = BooleanColumn.builder().add(true).addAll(true, false, true).build();
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
public final class BooleanColumnBuilder extends AbstractColumnBuilder<Boolean, BooleanColumn, BooleanColumnBuilder> {

	BooleanColumnBuilder() {
		super(0);
	}

	private int nonNullSize = 0;
	private BufferBitSet elements = new BufferBitSet();

	@Override
	void addNonNull(Boolean element) {
		add(element.booleanValue());
	}

	/**
	 * Adds a single {@code boolean} to the column.
	 *
	 * @param element the {@code boolean} to add
	 * 
	 * @return this builder
	 */
	public BooleanColumnBuilder add(boolean element) {
		if (element)
			elements.set(nonNullSize);
		size++;
		nonNullSize++;
		return this;
	}

	/**
	 * Adds a sequence of {@code booleans} to the column.
	 *
	 * @param elements the {@code booleans} to add
	 * 
	 * @return this builder
	 */
	public BooleanColumnBuilder addAll(boolean... elements) {
		for (int i = 0; i < elements.length; i++) {
			if (elements[i])
				this.elements.set(nonNullSize);
			nonNullSize++;
		}
		size += elements.length;
		return this;
	}

	@Override
	void ensureAdditionalCapacity(int size) {
		// noop
	}

	@Override
	public BooleanColumnBuilder ensureCapacity(int minCapacity) {
		// noop
		return this;
	}

	@Override
	BooleanColumn emptyNonNull() {
		return EMPTY;
	}

	@Override
	int getNonNullSize() {
		return nonNullSize;
	}

	@Override
	void checkCharacteristics() {
		throw new IllegalStateException();
	}

	@Override
	BooleanColumn buildNonNullColumn(int characteristics) {
		return new NonNullBooleanColumn(elements, 0, nonNullSize, false);
	}

	@Override
	BooleanColumn wrapNullableColumn(BooleanColumn column, BufferBitSet nonNulls) {
		return new NullableBooleanColumn((NonNullBooleanColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<Boolean> getType() {
		return ColumnType.BOOLEAN;
	}

	@Override
	void append0(BooleanColumnBuilder tail) {

		BufferBitSet elements = tail.elements.shiftRight(this.nonNullSize);
		elements.or(this.elements);
		this.elements = elements;

		this.nonNullSize += tail.nonNullSize;
	}
}
