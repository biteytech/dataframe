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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.dataframe.Column.BASE_CHARACTERISTICS;
import static tech.bitey.dataframe.Pr.checkArgument;
import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BufferBitSet;

abstract class AbstractColumnBuilder<E extends Comparable<? super E>, C extends Column<E>, B extends AbstractColumnBuilder<E, C, B>>
		implements ColumnBuilder<E> {

	private static final int VALID_CHARACTERISTICS = NONNULL_CHARACTERISTICS | SORTED | DISTINCT;

	final int characteristics;

	enum CharacteristicValidation {
		NONE, OTR, BUILD
	}

	BufferBitSet nulls;

	int size = 0;

	AbstractColumnBuilder(int characteristics) {
		checkArgument((characteristics & ~VALID_CHARACTERISTICS) == 0, "invalid characteristics");

		characteristics |= BASE_CHARACTERISTICS;

		if ((characteristics & DISTINCT) != 0)
			characteristics |= SORTED;
		if ((characteristics & SORTED) != 0)
			characteristics |= NONNULL;

		this.characteristics = characteristics;
	}

	abstract C emptyNonNull();

	abstract int getNonNullSize();

	abstract C buildNonNullColumn(int characteristics);

	abstract C wrapNullableColumn(C column, BufferBitSet nonNulls);

	abstract void append0(B tail);

	B append(B tail) {
		checkArgument((this.characteristics & tail.characteristics) == this.characteristics,
				"incompatible characteristics");

		if (tail.size > 0) {
			if (tail.nulls != null) {
				BufferBitSet bothNulls = tail.nulls.shiftRight(this.size);
				if (this.nulls != null)
					bothNulls.or(this.nulls);
				this.nulls = bothNulls;
			}

			append0(tail);

			this.size += tail.size;
		}

		@SuppressWarnings("unchecked")
		B cast = (B) this;
		return cast;
	}

	@Override
	public C build() {

		if (size == 0)
			return emptyNonNull();

		final C column;

		if (getNonNullSize() == 0) {
			column = emptyNonNull();
		} else {
			if (sorted() && getCharacteristicValidation() == CharacteristicValidation.BUILD)
				checkCharacteristics();

			column = buildNonNullColumn(characteristics);
		}

		if (nulls == null)
			return column;
		else {
			BufferBitSet nonNulls = new BufferBitSet();

			for (int i = 0; i < size; i++)
				if (!nulls.get(i))
					nonNulls.set(i);

			C nullable = wrapNullableColumn(column, nonNulls);
			return nullable;
		}
	}

	abstract void addNonNull(E element);

	abstract int compareToLast(E element);

	abstract void checkCharacteristics();

	abstract CharacteristicValidation getCharacteristicValidation();

	@Override
	public B addNulls(int count) {
		checkArgument(count >= 0, "count cannot be negative");

		if (nonNull())
			throw new NullPointerException("cannot add null when NONNULL is set");

		if (nulls == null)
			nulls = new BufferBitSet();

		nulls.set(size, size += count);

		@SuppressWarnings("unchecked")
		B b = (B) this;
		return b;
	}

	@Override
	public B addNull() {
		return addNulls(1);
	}

	@Override
	public B add(E element) {

		if (element == null)
			return addNull();

		if (size > 0 && sorted() && getCharacteristicValidation() == CharacteristicValidation.OTR) {
			// check that adding this element maintains SORTED/DISTINCT
			int c = compareToLast(element);

			if (distinct())
				Pr.checkArgument(c < 0, "adding element would violate DISTINCT characteristic");
			else
				Pr.checkArgument(c <= 0, "adding element would violate SORTED characteristic");
		}

		addNonNull(element);

		@SuppressWarnings("unchecked")
		B b = (B) this;
		return b;
	}

	@SuppressWarnings("unchecked")
	@Override
	public B add(E element, E... rest) {
		add(element);
		return addAll(rest);
	}

	abstract void ensureAdditionalCapacity(int additionalCapacity);

	@Override
	public B addAll(E[] elements) {

		int nonNullCount = 0;
		for (E e : elements)
			if (e != null)
				nonNullCount++;

		if (nonNullCount > 0)
			ensureAdditionalCapacity(nonNullCount);

		for (E e : elements)
			add(e);

		@SuppressWarnings("unchecked")
		B b = (B) this;
		return b;
	}

	@Override
	public B addAll(Collection<E> elements) {

		if (elements instanceof List && elements instanceof RandomAccess) {

			List<E> list = (List<E>) elements;

			int nonNullCount = 0;
			for (int i = 0; i < list.size(); i++)
				if (list.get(i) != null)
					nonNullCount++;

			if (nonNullCount > 0)
				ensureAdditionalCapacity(nonNullCount);
		}

		return addAll(elements.iterator());
	}

	@Override
	public B addAll(Iterator<E> elements) {

		while (elements.hasNext())
			add(elements.next());

		@SuppressWarnings("unchecked")
		B b = (B) this;
		return b;
	}

	@Override
	public B addAll(Iterable<E> elements) {
		if (elements instanceof Collection)
			return addAll((Collection<E>) elements);
		else
			return addAll(elements.iterator());
	}

	@Override
	public int size() {
		return size;
	}

	boolean nonNull() {
		return (characteristics & NONNULL) != 0;
	}

	boolean sorted() {
		return (characteristics & Spliterator.SORTED) != 0;
	}

	boolean distinct() {
		return (characteristics & Spliterator.DISTINCT) != 0;
	}
}
