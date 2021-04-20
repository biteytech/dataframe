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

import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

class FileDataFrameHeader {

	private static final long MAGIC_NUMBER = ((long) 'd') << 56 | ((long) 'a') << 48 | ((long) 't') << 40
			| ((long) 'a') << 32 | 'f' << 24 | 'r' << 16 | 'a' << 8 | 'm';

	private static final int VERSION = 1;

	private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

	private final long magicNumber;
	private final int version;
	private final int columnCount;
	private final int keyIndex;

	FileDataFrameHeader(DataFrame df) {
		this.magicNumber = MAGIC_NUMBER;
		this.version = VERSION;

		this.columnCount = df.columnCount();
		this.keyIndex = df.hasKeyColumn() ? df.keyColumnIndex() : -1;
	}

	FileDataFrameHeader(ReadableByteChannel fileChannel) throws IOException {

		ByteBuffer b = allocate();
		fileChannel.read(b);
		b.flip();

		magicNumber = b.getLong();
		checkState(magicNumber == MAGIC_NUMBER, "bad magic number: " + magicNumber);

		version = b.getInt();
		checkState(version == VERSION, "bad version: " + version);

		columnCount = b.getInt();
		checkState(columnCount > 0, "column count must be > 0: " + columnCount);

		keyIndex = b.getInt();
		checkState(keyIndex >= -1 && keyIndex < columnCount, "keyIndex must be >= -1 and < column count: " + keyIndex);
	}

	void writeTo(WritableByteChannel fileChannel) throws IOException {

		ByteBuffer b = allocate();

		b.putLong(magicNumber);
		b.putInt(version);
		b.putInt(columnCount);
		b.putInt(keyIndex);

		b.flip();

		fileChannel.write(b);
	}

	private ByteBuffer allocate() {
		ByteBuffer b = ByteBuffer.allocate(8 + 4 + 4 + 4);
		b.order(ORDER);
		return b;
	}

	@Override
	public String toString() {
		return "{version: " + version + ", columnCount: " + columnCount + ", keyIndex: " + keyIndex + "}";
	}

	int getVersion() {
		return version;
	}

	int getColumnCount() {
		return columnCount;
	}

	Integer keyIndex() {
		return keyIndex == -1 ? null : keyIndex;
	}
}
