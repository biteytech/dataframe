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

import java.util.Spliterator;

import tech.bitey.bufferstuff.BigByteBuffer;

@SuppressWarnings("unchecked")
abstract class VarLenColumnBuilder<E extends Comparable<? super E>, C extends Column<E>, B extends VarLenColumnBuilder<E, C, B>>
		extends AbstractColumnBuilder<E, C, B> {

	final IntColumnBuilder pointers = new IntColumnBuilder(Spliterator.NONNULL);
	final ByteColumnBuilder elements = new ByteColumnBuilder(Spliterator.NONNULL);

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

		pointers.add(elements.size());
		elements.addAll(packer.pack(element));

		size++;
		last = element;
	}

	@Override
	void ensureAdditionalCapacity(int additionalCapacity) {
		pointers.ensureAdditionalCapacity(additionalCapacity);
	}

	@Override
	public B ensureCapacity(int minCapacity) {
		pointers.ensureCapacity(minCapacity);
		return (B) this;
	}

	@Override
	int getNonNullSize() {
		return pointers.size();
	}

	@Override
	void append0(B tail) {

		int offset = elements.size();
		int begin = pointers.size();
		int end = begin + tail.pointers.size();

		elements.append(tail.elements);
		pointers.append(tail.pointers);

		for (int i = begin; i < end; i++)
			pointers.elements.put(i, pointers.elements.get(i) + offset);

		if (sorted())
			this.last = tail.last;
	}

	abstract C construct(BigByteBuffer elements, BigByteBuffer pointers, int characteristics, int size);

	@Override
	C buildNonNullColumn(int characteristics) {

		NonNullIntColumn pointers = (NonNullIntColumn) this.pointers.build();
		NonNullByteColumn elements = (NonNullByteColumn) this.elements.build();

		return construct(elements.buffer, pointers.buffer, characteristics, pointers.size());
	}
}
