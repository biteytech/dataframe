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

import static java.util.Spliterator.NONNULL;
import static tech.bitey.dataframe.Pr.checkState;

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
public final class UuidColumnBuilder extends AbstractColumnBuilder<UUID, UuidColumn, UuidColumnBuilder> {

	private final LongColumnBuilder msb = LongColumn.builder(NONNULL);
	private final LongColumnBuilder lsb = LongColumn.builder(NONNULL);

	UuidColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	public ColumnType<UUID> getType() {
		return ColumnType.UUID;
	}

	@Override
	public UuidColumnBuilder ensureCapacity(int minCapacity) {
		msb.ensureCapacity(minCapacity);
		lsb.ensureCapacity(minCapacity);
		return this;
	}

	@Override
	UuidColumn emptyNonNull() {
		return NonNullUuidColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	int getNonNullSize() {
		return msb.size();
	}

	@Override
	UuidColumn wrapNullableColumn(UuidColumn column, BufferBitSet nonNulls) {
		return new NullableUuidColumn((NonNullUuidColumn) column, nonNulls, null, 0, size);
	}

	@Override
	UuidColumn buildNonNullColumn(int characteristics) {
		return new NonNullUuidColumn((NonNullLongColumn) msb.build(), (NonNullLongColumn) lsb.build(), 0,
				getNonNullSize(), characteristics, false);
	}

	@Override
	void append0(UuidColumnBuilder tail) {
		msb.append(tail.msb);
		lsb.append(tail.lsb);
	}

	@Override
	void addNonNull(UUID element) {
		msb.add(element.getMostSignificantBits());
		lsb.add(element.getLeastSignificantBits());
		size++;
	}

	public void add(long msb, long lsb) {
		ensureAdditionalCapacity(1);
		this.msb.add(msb);
		this.lsb.add(lsb);
		size++;
	}

	@Override
	int compareToLast(UUID element) {
		throw new UnsupportedOperationException("compareToLast");
	}

	@Override
	void checkCharacteristics() {
		if (sorted() && size >= 2) {
			if (distinct())
				checkState(checkDistinct(), "column elements must be sorted and distinct");
			else
				checkState(checkSorted(), "column elements must be sorted");
		}
	}

	private boolean checkSorted() {

		for (int i = 1; i < size; i++)
			if (compareValuesAt(i - 1, i) > 0)
				return false;

		return true;
	}

	private boolean checkDistinct() {

		for (int i = 1; i < size; i++)
			if (compareValuesAt(i - 1, i) >= 0)
				return false;

		return true;
	}

	int compareValuesAt(int l, int r) {

		long L = msb.elements.get(l);
		long R = msb.elements.get(r);

		if (L < R)
			return -1;
		else if (L > R)
			return 1;
		else {
			L = lsb.elements.get(l);
			R = lsb.elements.get(r);

			if (L < R)
				return -1;
			else if (L > R)
				return 1;
			else
				return 0;
		}
	}

	@Override
	CharacteristicValidation getCharacteristicValidation() {
		return CharacteristicValidation.BUILD;
	}

	@Override
	void ensureAdditionalCapacity(int additionalCapacity) {
		msb.ensureAdditionalCapacity(additionalCapacity);
		lsb.ensureAdditionalCapacity(additionalCapacity);
	}

}
