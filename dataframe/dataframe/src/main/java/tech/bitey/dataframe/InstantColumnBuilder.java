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

import java.time.Instant;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link InstantColumn} instances. Example:
 *
 * <pre>
 * InstantColumn column = InstantColumn.builder().add(Instant.now()).build();
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
public final class InstantColumnBuilder extends FixedLenColumnBuilder<Instant, InstantColumn, InstantColumnBuilder> {

	InstantColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(Instant element) {
		add(element.getEpochSecond(), element.getNano());
	}

	public void add(long seconds, int nanos) {
		ensureAdditionalCapacity(1);
		buffer.putLong(seconds);
		buffer.putInt(nanos);
		size++;
	}

	@Override
	InstantColumn emptyNonNull() {
		return NonNullInstantColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	private long second(int index) {
		return buffer.getLong((long) index * 12);
	}

	private int nano(int index) {
		return buffer.getInt((long) index * 12 + 8);
	}

	@Override
	int compareValuesAt(int l, int r) {

		return (second(l) < second(r) ? -1
				: (second(l) > second(r) ? 1 : (nano(l) < nano(r) ? -1 : (nano(l) > nano(r) ? 1 : 0))));
	}

	@Override
	InstantColumn wrapNullableColumn(InstantColumn column, BufferBitSet nonNulls) {
		return new NullableInstantColumn((NonNullInstantColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<Instant> getType() {
		return ColumnType.INSTANT;
	}

	@Override
	InstantColumn buildNonNullColumn(BigByteBuffer trim, int characteristics) {
		return new NonNullInstantColumn(trim, 0, getNonNullSize(), characteristics, false);
	}

	@Override
	int elementSize() {
		return 12;
	}
}
