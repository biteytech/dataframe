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

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.SmallByteBuffer;

abstract class FixedLenColumnBuilder<E, C extends Column<E>, B extends FixedLenColumnBuilder<E, C, B>>
		extends SingleBufferColumnBuilder<E, SmallByteBuffer, C, B> {

	FixedLenColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	SmallByteBuffer asBuffer(BigByteBuffer buffer) {
		return null;
	}

	abstract int compareValuesAt(int l, int r);

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

	@Override
	int getNonNullSize() {
		return (int) (buffer.position() / elementSize());
	}

	@Override
	int getNonNullCapacity() {
		return (int) (buffer.capacity() / elementSize());
	}

	@Override
	void append0(B tail) {
		ensureAdditionalCapacity(tail.getNonNullSize());
		this.buffer.put(tail.buffer.duplicate().flip());
	}

	@Override
	void append00(SmallByteBuffer elements) {
		throw new UnsupportedOperationException();
	}
}
