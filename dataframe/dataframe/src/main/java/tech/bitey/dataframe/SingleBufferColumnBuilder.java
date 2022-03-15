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

import static tech.bitey.dataframe.Pr.checkState;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferUtils;
import tech.bitey.bufferstuff.SmallBuffer;

abstract class SingleBufferColumnBuilder<E extends Comparable<? super E>, F extends SmallBuffer, C extends Column<E>, B extends SingleBufferColumnBuilder<E, F, C, B>>
		extends AbstractColumnBuilder<E, C, B> {

	SingleBufferColumnBuilder(int characteristics) {
		super(characteristics);
	}

	BigByteBuffer buffer = allocate(8);
	F elements = asBuffer(buffer);

	abstract F asBuffer(BigByteBuffer buffer);

	abstract C buildNonNullColumn(BigByteBuffer trim, int characteristics);

	abstract int elementSize();

	private BigByteBuffer allocate(int capacity) {
		return BufferUtils.allocateBig((long) capacity * elementSize());
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
			BigByteBuffer extended = allocate(expandedCapacity);
			buffer.position((long) getNonNullSize() * elementSize());
			buffer.flip();
			extended.put(buffer);

			buffer = extended;
			elements = asBuffer(buffer);
		}
		return (B) this;
	}

	// from Guava's ImmutableCollection.Builder
	private int expandedCapacity(int oldCapacity, int minCapacity) {

		checkState(minCapacity >= 0, "minCapacity must be >= 0");

		// careful of overflow!
		int newCapacity = oldCapacity + (oldCapacity >> 1) + 1;
		if (newCapacity < minCapacity) {
			newCapacity = Integer.highestOneBit(minCapacity - 1) << 1;
		}
		if (newCapacity < 0) {
			newCapacity = Integer.MAX_VALUE;
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
		BigByteBuffer full = buffer.duplicate();
		full.flip();
		full.limit((long) getNonNullSize() * elementSize());

		BigByteBuffer trim = allocate(getNonNullSize());
		trim.put(full);
		trim.flip();

		return buildNonNullColumn(trim, characteristics);
	}

	abstract void append00(F elements);

	@Override
	void append0(B tail) {
		ensureAdditionalCapacity(tail.getNonNullSize());
		append00(tail.elements);
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
