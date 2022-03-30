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

import static tech.bitey.bufferstuff.BufferUtils.EMPTY_BIG_BUFFER;

import java.io.InputStream;

import tech.bitey.bufferstuff.BigByteBuffer;

final class NonNullBlobColumn extends NonNullVarLenColumn<InputStream, BlobColumn, NonNullBlobColumn>
		implements BlobColumn {

	static final NonNullBlobColumn EMPTY = new NonNullBlobColumn(EMPTY_BIG_BUFFER, EMPTY_BIG_BUFFER, 0, 0,
			NONNULL_CHARACTERISTICS, false);

	NonNullBlobColumn(BigByteBuffer elements, BigByteBuffer rawPointers, int offset, int size, int characteristics,
			boolean view) {
		super(VarLenPacker.BLOB, elements, rawPointers, offset, size, characteristics, view);
	}

	@Override
	NonNullBlobColumn construct(BigByteBuffer elements, BigByteBuffer rawPointers, int offset, int size,
			int characteristics, boolean view) {
		return new NonNullBlobColumn(elements, rawPointers, offset, size, characteristics, view);
	}

	@Override
	NonNullBlobColumn empty() {
		return EMPTY;
	}

	@Override
	public ColumnType<InputStream> getType() {
		return ColumnType.BLOB;
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof InputStream;
	}
}
