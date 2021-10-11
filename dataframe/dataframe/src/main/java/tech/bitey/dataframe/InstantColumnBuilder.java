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
import java.nio.IntBuffer;
import java.time.Instant;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link InstantColumn} instances. Example:
 *
 * <pre>
 * InstantColumn column = InstantColumn.builder().add(Instant.now()).build();
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
public final class InstantColumnBuilder
		extends SingleBufferColumnBuilder<Instant, IntBuffer, InstantColumn, InstantColumnBuilder> {

	InstantColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(Instant element) {
		add(element.getEpochSecond(), element.getNano());
	}

	public void add(long seconds, int nanos) {
		ensureAdditionalCapacity(1);
		elements.put((int) (seconds >> 32));
		elements.put((int) seconds);
		elements.put(nanos);
		size++;
	}

	@Override
	InstantColumn emptyNonNull() {
		return NonNullInstantColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
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

	private long second(int index) {
		return elements.get(index * 3) << 32L | elements.get(index * 3 + 1);
	}

	private int nano(int index) {
		return elements.get(index * 3 + 2);
	}

	int compareValuesAt(int l, int r) {

		return (second(l) < second(r) ? -1
				: (second(l) > second(r) ? 1 : (nano(l) < nano(r) ? -1 : (nano(l) > nano(r) ? 1 : 0))));
	}

	@Override
	InstantColumn wrapNullableColumn(InstantColumn column, BufferBitSet nonNulls) {
		return new NullableInstantColumn((NonNullInstantColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<Instant> getType() {
		return ColumnType.INSTANT;
	}

	@Override
	InstantColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullInstantColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	IntBuffer asBuffer(ByteBuffer buffer) {
		return buffer.asIntBuffer();
	}

	@Override
	int elementSize() {
		return 12;
	}

	@Override
	int getNonNullSize() {
		return elements.position() / 3;
	}

	@Override
	int getNonNullCapacity() {
		return elements.capacity() / 3;
	}

	@Override
	void append00(IntBuffer elements) {
		IntBuffer tail = elements.duplicate();
		tail.flip();
		this.elements.put(tail);
	}
}
