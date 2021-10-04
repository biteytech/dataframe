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
import java.util.ArrayList;

import tech.bitey.bufferstuff.BufferUtils;

@SuppressWarnings("unchecked")
abstract class VarLenColumnBuilder<E extends Comparable<? super E>, C extends Column<E>, B extends VarLenColumnBuilder<E, C, B>>
		extends AbstractColumnBuilder<E, C, B> {

	final ArrayList<byte[]> elements = new ArrayList<>();

	final VarLenPacker<E> packer;

	E last;

	VarLenColumnBuilder(int characteristics, VarLenPacker<E> packer) {
		super(characteristics);

		this.packer = packer;
	}

	@Override
	CharacteristicValidation getCharacteristicValidation() {
		return CharacteristicValidation.OTR;
	}

	@Override
	void checkCharacteristics() {
		throw new UnsupportedOperationException("checkCharacteristics");
	}

	@Override
	int compareToLast(E element) {
		return last.compareTo(element);
	}

	@Override
	void addNonNull(E element) {
		elements.add(packer.pack(element));
		size++;
		last = element;
	}

	@Override
	void ensureAdditionalCapacity(int additionalCapacity) {
		elements.ensureCapacity(elements.size() + additionalCapacity);
	}

	@Override
	public B ensureCapacity(int minCapacity) {
		elements.ensureCapacity(minCapacity);
		return (B) this;
	}

	@Override
	int getNonNullSize() {
		return elements.size();
	}

	@Override
	void append0(B tail) {
		this.elements.addAll(tail.elements);
		if (sorted())
			this.last = tail.last;
	}

	abstract C construct(ByteBuffer elements, ByteBuffer pointers, int characteristics, int size);

	@Override
	C buildNonNullColumn(int characteristics) {

		int byteLength = elements.stream().mapToInt(b -> b.length).sum();

		ByteBuffer pointers = BufferUtils.allocate(elements.size() * 4);
		ByteBuffer elements = BufferUtils.allocate(byteLength);

		int pointer = 0;
		for (byte[] packed : this.elements) {
			elements.put(packed);
			pointers.putInt(pointer);
			pointer += packed.length;
		}
		pointers.flip();
		elements.flip();

		return construct(elements, pointers, characteristics, this.elements.size());
	}
}
