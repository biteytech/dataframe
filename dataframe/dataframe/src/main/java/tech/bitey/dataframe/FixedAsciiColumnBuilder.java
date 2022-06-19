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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link FixedAsciiColumn} instances. Example:
 *
 * <pre>
 * FixedAsciiColumn column = FixedAsciiColumn.builder().add("hello", "world").build();
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
public final class FixedAsciiColumnBuilder
		extends FixedLenColumnBuilder<String, FixedAsciiColumn, FixedAsciiColumnBuilder> {

	private int width = 0;
	private int nonNullSize = 0;

	FixedAsciiColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(String element) {
		ByteBuffer bb = StandardCharsets.US_ASCII.encode(element);

		if (nonNullSize == 0)
			width = bb.capacity();
		else
			Pr.checkArgument(bb.capacity() == width, "mismatched ASCII string width");

		ensureAdditionalCapacity(1);

		buffer.put(bb);
		size++;
		nonNullSize++;
	}

	@Override
	FixedAsciiColumn emptyNonNull() {
		return NonNullFixedAsciiColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	int compareValuesAt(int l, int r) {

		final long fromL = (long) l * width;
		final long fromR = (long) r * width;

		return buffer.smallSlice(fromL, fromL + width).compareTo(buffer.smallSlice(fromR, fromR + width));
	}

	@Override
	FixedAsciiColumn wrapNullableColumn(FixedAsciiColumn column, BufferBitSet nonNulls) {
		return new NullableFixedAsciiColumn((NonNullFixedAsciiColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.FSTRING;
	}

	@Override
	FixedAsciiColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullFixedAsciiColumn(width, trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	int elementSize() {
		return width;
	}

	@Override
	int getNonNullSize() {
		return nonNullSize;
	}

	@Override
	int getNonNullCapacity() {
		return width == 0 ? 0 : super.getNonNullCapacity();
	}

	@Override
	void append0(FixedAsciiColumnBuilder tail) {
		if (nonNullSize == 0)
			this.width = tail.width;

		super.append0(tail);

		this.nonNullSize += tail.nonNullSize;
	}
}
