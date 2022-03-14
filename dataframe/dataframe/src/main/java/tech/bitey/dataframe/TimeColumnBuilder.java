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

import java.time.LocalTime;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link TimeColumn} instances. Example:
 *
 * <pre>
 * TimeColumn column = TimeColumn.builder().add(LocalTime.now()).build();
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
public final class TimeColumnBuilder extends LongArrayColumnBuilder<LocalTime, TimeColumn, TimeColumnBuilder> {

	TimeColumnBuilder(int characteristics) {
		super(characteristics, LongArrayPacker.LOCAL_TIME);
	}

	@Override
	TimeColumn emptyNonNull() {
		return NonNullTimeColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	TimeColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullTimeColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	TimeColumn wrapNullableColumn(TimeColumn column, BufferBitSet nonNulls) {
		return new NullableTimeColumn((NonNullTimeColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<LocalTime> getType() {
		return ColumnType.TIME;
	}
}
