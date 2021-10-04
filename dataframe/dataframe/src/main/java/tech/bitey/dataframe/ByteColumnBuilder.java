/*
 * Copyright 2021 biteytech@protonmail.com
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
import java.util.Spliterator;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link ByteColumn} instances. Example:
 *
 * <pre>
 * ByteColumn column = ByteColumn.builder().add((byte) 10).addAll((byte) 20, (byte) 30, (byte) 40).build();
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
public final class ByteColumnBuilder extends ByteArrayColumnBuilder<Byte, ByteColumn, ByteColumnBuilder> {

	ByteColumnBuilder(int characteristics) {
		super(characteristics, ByteArrayPacker.BYTE);
	}

	@Override
	ByteColumn emptyNonNull() {
		return NonNullByteColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	ByteColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullByteColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	ByteColumn wrapNullableColumn(ByteColumn column, BufferBitSet nonNulls) {
		return new NullableByteColumn((NonNullByteColumn) column, nonNulls, null, 0, size);
	}

	/**
	 * Adds a single {@code byte} to the column.
	 *
	 * @param element the {@code byte} to add
	 * 
	 * @return this builder
	 */
	public ByteColumnBuilder add(byte element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}

	/**
	 * Adds a sequence of {@code bytes} to the column.
	 *
	 * @param elements the {@code bytes} to add
	 * 
	 * @return this builder
	 */
	public ByteColumnBuilder addAll(byte... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	public ColumnType<Byte> getType() {
		return ColumnType.BYTE;
	}

	@Override
	void append00(ByteBuffer elements) {
		ByteBuffer tail = elements.duplicate();
		tail.flip();
		this.elements.put(tail);
	}
}
