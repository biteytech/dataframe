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

import static tech.bitey.bufferstuff.BufferUtils.readFully;
import static tech.bitey.bufferstuff.BufferUtils.writeFully;
import static tech.bitey.dataframe.Pr.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Spliterator;

class ChannelColumnHeader {

	private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

	private final byte[] columnName;
	private final byte[] columnType;
	private final int characteristics;

	ChannelColumnHeader(DataFrame df, int columnIndex) {

		this.columnName = df.columnName(columnIndex).getBytes(StandardCharsets.UTF_8);
		this.columnType = df.columnType(columnIndex).getCode().name().getBytes(StandardCharsets.UTF_8);
		this.characteristics = df.column(columnIndex).characteristics();
	}

	ChannelColumnHeader(ReadableByteChannel channel) throws IOException {

		ByteBuffer i = allocate(4);
		readFully(channel, i);
		columnName = new byte[i.getInt(0)];
		readFully(channel, ByteBuffer.wrap(columnName));

		i.clear();
		readFully(channel, i);
		columnType = new byte[i.getInt(0)];
		readFully(channel, ByteBuffer.wrap(columnType));

		i.clear();
		readFully(channel, i);
		characteristics = i.getInt(0);

		checkState((characteristics & AbstractColumn.BASE_CHARACTERISTICS) == AbstractColumn.BASE_CHARACTERISTICS,
				"bad characteristics: " + characteristics);
	}

	void writeTo(WritableByteChannel channel) throws IOException {

		ByteBuffer b = allocate(4 + columnName.length + 4 + columnType.length + 8);

		b.putInt(columnName.length);
		b.put(columnName);
		b.putInt(columnType.length);
		b.put(columnType);
		b.putInt(characteristics);

		b.flip();

		writeFully(channel, b);
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
		return new String(columnName, StandardCharsets.UTF_8);
	}

	ColumnType<?> getColumnType() {
		return ColumnTypeCode.valueOf(new String(columnType, StandardCharsets.UTF_8)).getType();
	}

	int getCharacteristics() {
		return characteristics;
	}
}
