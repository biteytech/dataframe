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

import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Spliterator;

class FileColumnHeader {

	private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

	private static final Charset CHARSET = Charset.forName("UTF-8");

	private final byte[] columnName;
	private final byte[] columnType;
	private final int characteristics;

	FileColumnHeader(DataFrame df, int columnIndex) {

		this.columnName = df.columnName(columnIndex).getBytes(CHARSET);
		this.columnType = df.columnType(columnIndex).getCode().name().getBytes(CHARSET);
		this.characteristics = df.column(columnIndex).characteristics();
	}

	FileColumnHeader(ReadableByteChannel fileChannel) throws IOException {

		ByteBuffer i = allocate(4);
		fileChannel.read(i);
		columnName = new byte[i.getInt(0)];
		fileChannel.read(ByteBuffer.wrap(columnName));

		i.clear();
		fileChannel.read(i);
		columnType = new byte[i.getInt(0)];
		fileChannel.read(ByteBuffer.wrap(columnType));

		i.clear();
		fileChannel.read(i);
		characteristics = i.getInt(0);

		checkState((characteristics & AbstractColumn.BASE_CHARACTERISTICS) == AbstractColumn.BASE_CHARACTERISTICS,
				"bad characteristics: " + characteristics);
	}

	void writeTo(WritableByteChannel fileChannel) throws IOException {

		ByteBuffer b = allocate(4 + columnName.length + 4 + columnType.length + 8);

		b.putInt(columnName.length);
		b.put(columnName);
		b.putInt(columnType.length);
		b.put(columnType);
		b.putInt(characteristics);

		b.flip();

		fileChannel.write(b);
	}

	private ByteBuffer allocate(int capacity) {
		ByteBuffer b = ByteBuffer.allocate(capacity);
		b.order(ORDER);
		return b;
	}

	@Override
	public String toString() {
		final String c;
		if ((characteristics & Spliterator.DISTINCT) != 0)
			c = "DISTINCT";
		else if ((characteristics & Spliterator.SORTED) != 0)
			c = "SORTED";
		else if ((characteristics & Spliterator.NONNULL) != 0)
			c = "NONNULL";
		else
			c = "NULLABLE";

		return "{columnName: " + getColumnName() + ", columnType: " + getColumnType() + ", characteristics: "
				+ characteristics + " (" + c + ")}";
	}

	String getColumnName() {
		return new String(columnName, CHARSET);
	}

	ColumnType<?> getColumnType() {
		return ColumnTypeCode.valueOf(new String(columnType, CHARSET)).getType();
	}

	int getCharacteristics() {
		return characteristics;
	}
}
