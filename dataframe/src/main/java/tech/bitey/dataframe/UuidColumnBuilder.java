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
import java.nio.LongBuffer;
import java.util.Spliterator;
import java.util.UUID;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link UuidColumn} instances. Example:
 *
 * <pre>
 * UuidColumn column = UuidColumn.builder().add(UUID.randomUUID()).build();
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
public final class UuidColumnBuilder
		extends SingleBufferColumnBuilder<UUID, LongBuffer, UuidColumn, UuidColumnBuilder> {

	UuidColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(UUID element) {
		add(element.getMostSignificantBits(), element.getLeastSignificantBits());
	}

	public void add(long msb, long lsb) {
		ensureAdditionalCapacity(1);
		elements.put(msb);
		elements.put(lsb);
		size++;
	}

	@Override
	UuidColumn emptyNonNull() {
		return NonNullUuidColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	boolean checkSorted() {

		for (int i = 1; i < size; i++)
			if (compareValuesAt(i - 1, i) > 0)
				return false;

		return true;
	}

	@Override
	boolean checkDistinct() {

		for (int i = 1; i < size; i++)
			if (compareValuesAt(i - 1, i) >= 0)
				return false;

		return true;
	}

	private long msb(int index) {
		return elements.get(index << 1);
	}

	private long lsb(int index) {
		return elements.get((index << 1) + 1);
	}

	int compareValuesAt(int l, int r) {

		return (msb(l) < msb(r) ? -1 : (msb(l) > msb(r) ? 1 : (lsb(l) < lsb(r) ? -1 : (lsb(l) > lsb(r) ? 1 : 0))));
	}

	@Override
	UuidColumn wrapNullableColumn(UuidColumn column, BufferBitSet nonNulls) {
		return new NullableUuidColumn((NonNullUuidColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<UUID> getType() {
		return ColumnType.UUID;
	}

	@Override
	UuidColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullUuidColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	LongBuffer asBuffer(ByteBuffer buffer) {
		return buffer.asLongBuffer();
	}

	@Override
	int elementSize() {
		return 16;
	}

	@Override
	int getNonNullSize() {
		return elements.position() / 2;
	}

	@Override
	int getNonNullCapacity() {
		return elements.capacity() / 2;
	}
}
