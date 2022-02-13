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

import java.math.BigDecimal;

import tech.bitey.bufferstuff.BufferBitSet;

final class NullableDecimalColumn
		extends NullableVarLenColumn<BigDecimal, DecimalColumn, NonNullDecimalColumn, NullableDecimalColumn>
		implements DecimalColumn {

	NullableDecimalColumn(NonNullColumn<BigDecimal, DecimalColumn, NonNullDecimalColumn> column, BufferBitSet nonNulls,
			INullCounts nullCounts, int offset, int size) {
		super((NonNullDecimalColumn) column, nonNulls, nullCounts, offset, size);
	}
}
