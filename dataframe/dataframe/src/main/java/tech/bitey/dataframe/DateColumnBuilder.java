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

import java.time.LocalDate;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link DateColumn} instances. Example:
 *
 * <pre>
 * DateColumn column = DateColumn.builder().add(LocalDate.now()).build();
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
public final class DateColumnBuilder extends IntArrayColumnBuilder<LocalDate, DateColumn, DateColumnBuilder> {

	DateColumnBuilder(int characteristics) {
		super(characteristics, IntArrayPacker.LOCAL_DATE);
	}

	@Override
	DateColumn emptyNonNull() {
		return NonNullDateColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	DateColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullDateColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	DateColumn wrapNullableColumn(DateColumn column, BufferBitSet nonNulls) {
		return new NullableDateColumn((NonNullDateColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<LocalDate> getType() {
		return ColumnType.DATE;
	}

	/**
	 * Adds a date to the column.
	 *
	 * @param year  - the year (yyyy)
	 * @param month - the month, 1-based
	 * @param day   - the day, 1-based
	 * 
	 * @return this builder
	 */
	public DateColumnBuilder add(int year, int month, int day) {
		ensureAdditionalCapacity(1);
		elements.put(IntArrayPacker.packDate(year, month, day));
		size++;
		return this;
	}
}
