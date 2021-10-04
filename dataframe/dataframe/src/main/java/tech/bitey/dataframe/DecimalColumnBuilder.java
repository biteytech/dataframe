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

import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link DecimalColumn} instances. Example:
 *
 * <pre>
 * DecimalColumn column = DecimalColumn.builder().add(BigDecimal.ZERO).add(new BigDecimal("42.42").build();
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
public final class DecimalColumnBuilder extends VarLenColumnBuilder<BigDecimal, DecimalColumn, DecimalColumnBuilder> {

	DecimalColumnBuilder(int characteristics) {
		super(characteristics, VarLenPacker.DECIMAL);
	}

	@Override
	DecimalColumn emptyNonNull() {
		return NonNullDecimalColumn.EMPTY.get(NONNULL_CHARACTERISTICS);
	}

	@Override
	DecimalColumn wrapNullableColumn(DecimalColumn column, BufferBitSet nonNulls) {
		return new NullableDecimalColumn((NonNullDecimalColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<BigDecimal> getType() {
		return ColumnType.DECIMAL;
	}

	@Override
	DecimalColumn construct(ByteBuffer elements, ByteBuffer pointers, int characteristics, int size) {
		return new NonNullDecimalColumn(elements, pointers, 0, size, characteristics, false);
	}
}
