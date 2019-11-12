/*
 * Copyright 2019 biteytech@protonmail.com
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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.dataframe.DfPreconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import tech.bitey.bufferstuff.BufferUtils;

@SuppressWarnings("unchecked")
abstract class VarLenColumnBuilder<E extends Comparable<E>, C extends Column<E>, B extends VarLenColumnBuilder<E, C, B>>
		extends AbstractColumnBuilder<E, C, B> {

	final ArrayList<E> elements = new ArrayList<>();

	final VarLenPacker<E> packer;

	VarLenColumnBuilder(int characteristics, VarLenPacker<E> packer) {
		super(characteristics);

		this.packer = packer;
	}

	@Override
	void addNonNull(E element) {
		elements.add(element);
		size++;
	}

	@Override
	void ensureAdditionalCapacity(int size) {
		elements.ensureCapacity(elements.size() + size);
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
	void checkCharacteristics() {
		if (elements.size() >= 2) {
			E prev = elements.get(0);

			if ((characteristics & DISTINCT) != 0) {
				for (int i = 1; i < elements.size(); i++) {
					E e = elements.get(i);
					checkArgument(prev.compareTo(e) < 0, "column elements must be sorted and distinct");
					prev = e;
				}
			} else if ((characteristics & SORTED) != 0) {
				for (int i = 1; i < elements.size(); i++) {
					E e = elements.get(i);
					checkArgument(prev.compareTo(e) <= 0, "column elements must be sorted");
					prev = e;
				}
			}
		}
	}

	@Override
	void append0(B tail) {
		this.elements.addAll(tail.elements);
	}

	void sort() {
		Collections.sort(elements);
		characteristics |= SORTED;
	}

	void distinct() {
		// TODO: make this more efficient
		TreeSet<E> distinct = new TreeSet<>(elements);
		elements.clear();
		elements.addAll(distinct);
		size = elements.size();
		characteristics |= SORTED | DISTINCT;		
	}
	
	abstract C construct(ByteBuffer elements, ByteBuffer pointers, int characteristics);

	@Override
	C buildNonNullColumn(int characteristics) {

		List<byte[]> packedList = new ArrayList<byte[]>(elements.size());
		int byteLength = 0;
		for (int i = 0; i < elements.size(); i++) {
			byte[] packed = packer.pack(elements.get(i));
			byteLength += packed.length;
			packedList.add(packed);
		}

		ByteBuffer elements = BufferUtils.allocate(byteLength);
		ByteBuffer pointers = BufferUtils.allocate(packedList.size() * 4);

		int pointer = 0;
		for (byte[] packed : packedList) {
			elements.put(packed);
			pointers.putInt(pointer);
			pointer += packed.length;
		}
		pointers.flip();
		elements.flip();

		return construct(elements, pointers, characteristics);
	}
}
