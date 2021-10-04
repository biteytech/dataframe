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
import java.nio.LongBuffer;

import tech.bitey.bufferstuff.BufferUtils;

abstract class LongArrayColumnBuilder<E extends Comparable<? super E>, C extends Column<E>, B extends LongArrayColumnBuilder<E, C, B>>
		extends SingleBufferColumnBuilder<E, LongBuffer, C, B> {

	private final LongArrayPacker<E> packer;

	LongArrayColumnBuilder(int characteristics, LongArrayPacker<E> packer) {
		super(characteristics);
		this.packer = packer;
	}

	@Override
	void addNonNull(E element) {
		ensureAdditionalCapacity(1);
		elements.put(packer.pack(element));
		size++;
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
	LongBuffer asBuffer(ByteBuffer buffer) {
		return buffer.asLongBuffer();
	}

	@Override
	int elementSize() {
		return 8;
	}

	@Override
	void append00(LongBuffer elements) {
		LongBuffer tail = elements.duplicate();
		tail.flip();
		this.elements.put(tail);
	}
}
