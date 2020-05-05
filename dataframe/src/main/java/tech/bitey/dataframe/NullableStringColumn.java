/*
 * Copyright 2020 biteytech@protonmail.com
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

import static tech.bitey.bufferstuff.BufferBitSet.EMPTY_BITSET;
import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.nio.IntBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableStringColumn extends
		NullableVarLenColumn<String, StringColumn, NonNullStringColumn, NullableStringColumn> implements StringColumn {

	static final NullableStringColumn EMPTY = new NullableStringColumn(
			NonNullStringColumn.EMPTY.get(NONNULL_CHARACTERISTICS), EMPTY_BITSET, null, 0, 0);

	NullableStringColumn(NonNullStringColumn column, BufferBitSet nonNulls, IntBuffer nullCounts, int offset,
			int size) {
		super(column, nonNulls, nullCounts, offset, size);
	}

	@Override
	NullableStringColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableStringColumn(column, nonNulls, nullCounts, fromIndex + offset, toIndex - fromIndex);
	}

	@Override
	NullableStringColumn empty() {
		return EMPTY;
	}

	@Override
	NullableStringColumn construct(NonNullStringColumn column, BufferBitSet nonNulls, int size) {
		return new NullableStringColumn(column, nonNulls, null, 0, size);
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof String;
	}

	@Override
	public BooleanColumn parseBoolean() {
		return NullableBooleanColumn.EMPTY.construct((NonNullBooleanColumn) subColumn.parseBoolean(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public DateColumn parseDate() {
		return NullableDateColumn.EMPTY.construct((NonNullDateColumn) subColumn.parseDate(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public DateTimeColumn parseDateTime() {
		return NullableDateTimeColumn.EMPTY.construct((NonNullDateTimeColumn) subColumn.parseDateTime(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public DoubleColumn parseDouble() {
		return NullableDoubleColumn.EMPTY.construct((NonNullDoubleColumn) subColumn.parseDouble(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public FloatColumn parseFloat() {
		return NullableFloatColumn.EMPTY.construct((NonNullFloatColumn) subColumn.parseFloat(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public IntColumn parseInt() {
		return NullableIntColumn.EMPTY.construct((NonNullIntColumn) subColumn.parseInt(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public LongColumn parseLong() {
		return NullableLongColumn.EMPTY.construct((NonNullLongColumn) subColumn.parseLong(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public ShortColumn parseShort() {
		return NullableShortColumn.EMPTY.construct((NonNullShortColumn) subColumn.parseShort(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public ByteColumn parseByte() {
		return NullableByteColumn.EMPTY.construct((NonNullByteColumn) subColumn.parseByte(),
				nonNulls.get(offset, offset + size), size);
	}

	@Override
	public DecimalColumn parseDecimal() {
		return NullableDecimalColumn.EMPTY.construct((NonNullDecimalColumn) subColumn.parseDecimal(),
				nonNulls.get(offset, offset + size), size);
	}
}
