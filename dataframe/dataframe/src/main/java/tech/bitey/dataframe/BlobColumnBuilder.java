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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link BlobColumn} instances.
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
public final class BlobColumnBuilder extends VarLenColumnBuilder<InputStream, BlobColumn, BlobColumnBuilder> {

	BlobColumnBuilder(int characteristics) {
		super(characteristics, VarLenPacker.BLOB);
	}

	@Override
	BlobColumn emptyNonNull() {
		return NonNullBlobColumn.EMPTY;
	}

	@Override
	BlobColumn wrapNullableColumn(BlobColumn column, BufferBitSet nonNulls) {
		return new NullableBlobColumn((NonNullBlobColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType<InputStream> getType() {
		return ColumnType.BLOB;
	}

	@Override
	NonNullBlobColumn construct(BigByteBuffer elements, BigByteBuffer pointers, int characteristics, int size) {
		return new NonNullBlobColumn(elements, pointers, 0, size, characteristics, false);
	}

	@Override
	int compareToLast(InputStream element) {
		throw new UnsupportedOperationException();
	}

	public BlobColumnBuilder add(byte[] bytes, int offset, int length) {
		return add(new ByteArrayInputStream(bytes, offset, length));
	}

	public BlobColumnBuilder add(byte[] bytes) {
		return add(new ByteArrayInputStream(bytes));
	}
}
