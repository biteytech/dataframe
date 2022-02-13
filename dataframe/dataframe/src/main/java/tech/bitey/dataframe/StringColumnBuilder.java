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

import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.nio.ByteBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link StringColumn} instances. Example:
 *
 * <pre>
 * StringColumn column = StringColumn.builder().add("Hello").add("World", "!").build();
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
public final class StringColumnBuilder extends VarLenColumnBuilder<String, StringColumn, StringColumnBuilder> {

	StringColumnBuilder(int characteristics) {
		super(characteristics, VarLenPacker.STRING);
	}

	@Override
	StringColumn emptyNonNull() {
		return NonNullStringColumn.EMPTY.get(NONNULL_CHARACTERISTICS);
	}

	@Override
	StringColumn wrapNullableColumn(StringColumn column, BufferBitSet nonNulls) {
		return new NullableStringColumn((NonNullStringColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.STRING;
	}

	@Override
	StringColumn construct(ByteBuffer elements, ByteBuffer pointers, int characteristics, int size) {
		return new NonNullStringColumn(elements, pointers, 0, size, characteristics, false);
	}
}
