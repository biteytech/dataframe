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

import static tech.bitey.bufferstuff.BufferUtils.duplicate;
import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import tech.bitey.bufferstuff.BufferUtils;

abstract class SingleBufferColumnBuilder<E extends Comparable<? super E>, F extends Buffer, C extends Column<E>, B extends SingleBufferColumnBuilder<E, F, C, B>>
		extends AbstractColumnBuilder<E, C, B> {

	/**
	 * From {@code AbstractCollection}
	 * <p>
	 * The maximum size of array to allocate. Some VMs reserve some header words in
	 * an array. Attempts to allocate larger arrays may result in OutOfMemoryError:
	 * Requested array size exceeds VM limit
	 */
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

	SingleBufferColumnBuilder(int characteristics) {
		super(characteristics);
	}

	ByteBuffer buffer = allocate(10);
	F elements = asBuffer(buffer);

	abstract F asBuffer(ByteBuffer buffer);

	abstract C buildNonNullColumn(ByteBuffer trim, int characteristics);

	abstract int elementSize();

	private void resetElementBuffer() {
		ByteBuffer buffer = duplicate(this.buffer);
		buffer.clear();
		int position = elements.position();
		elements = asBuffer(buffer);
		elements.position(position);
	}

	private ByteBuffer allocate(int capacity) {
		return BufferUtils.allocate(capacity * elementSize());
	}

	@Override
	void ensureAdditionalCapacity(int additionalCapacity) {
		ensureCapacity(getNonNullSize() + additionalCapacity);
	}

	@SuppressWarnings("unchecked")
	@Override
	public B ensureCapacity(int minCapacity) {
		if (getNonNullCapacity() < minCapacity) {

			int expandedCapacity = expandedCapacity(getNonNullCapacity(), minCapacity);
			ByteBuffer extended = allocate(expandedCapacity);
			buffer.position(getNonNullSize() * elementSize());
			buffer.flip();
			extended.put(buffer);

			buffer = extended;
			resetElementBuffer();
		}
		return (B) this;
	}

	// from Guava's ImmutableCollection.Builder
	private int expandedCapacity(int oldCapacity, int minCapacity) {

		final int maxCapacity = MAX_ARRAY_SIZE / elementSize();

		checkState(minCapacity >= 0 && minCapacity <= maxCapacity,
				"cannot store more than " + maxCapacity + " elements");

		// careful of overflow!
		int newCapacity = oldCapacity + (oldCapacity >> 1) + 1;
		if (newCapacity < minCapacity) {
			newCapacity = Integer.highestOneBit(minCapacity - 1) << 1;
		}
		if (newCapacity < 0 || newCapacity > maxCapacity) {
			newCapacity = maxCapacity;
		}
		return newCapacity;
	}

	@Override
	int getNonNullSize() {
		return elements.position();
	}

	int getNonNullCapacity() {
		return elements.capacity();
	}

	@Override
	C buildNonNullColumn(int characteristics) {
		ByteBuffer full = duplicate(buffer);
		full.flip();
		full.limit(getNonNullSize() * elementSize());

		ByteBuffer trim = allocate(getNonNullSize());
		trim.put(full);
		trim.flip();

		return buildNonNullColumn(trim, characteristics);
	}

	@Override
	void append0(B tail) {
		ensureAdditionalCapacity(tail.getNonNullSize());

		ByteBuffer tailBuffer = duplicate(tail.buffer);
		tailBuffer.flip();
		buffer.put(tailBuffer);
		resetElementBuffer();
	}

	@Override
	CharacteristicValidation getCharacteristicValidation() {
		return CharacteristicValidation.BUILD;
	}

	@Override
	int compareToLast(E element) {
		throw new UnsupportedOperationException("compareToLast");
	}

	abstract boolean checkSorted();

	abstract boolean checkDistinct();

	@Override
	void checkCharacteristics() {
		if (sorted() && size >= 2) {
			if (distinct())
				checkState(checkDistinct(), "column elements must be sorted and distinct");
			else
				checkState(checkSorted(), "column elements must be sorted");
		}
	}
}
