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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import tech.bitey.bufferstuff.BigByteBuffer;
import tech.bitey.bufferstuff.ResizableBigByteBuffer;

interface VarLenPacker<E> {

	final VarLenPacker<String> STRING = new VarLenPacker<String>() {
		@Override
		public void pack(ResizableBigByteBuffer buffer, String value) {
			buffer.put(UTF_8.encode(value));
		}

		@Override
		public String unpack(BigByteBuffer buffer) {
			return UTF_8.decode(buffer.smallSlice()).toString();
		}
	};

	final VarLenPacker<BigDecimal> DECIMAL = new VarLenPacker<BigDecimal>() {
		@Override
		public void pack(ResizableBigByteBuffer buffer, BigDecimal value) {

			buffer.putInt(value.scale());
			buffer.put(value.unscaledValue().toByteArray());
		}

		@Override
		public BigDecimal unpack(BigByteBuffer buffer) {

			ByteBuffer bb = buffer.smallSlice();

			int scale = bb.getInt();

			byte[] unscaledbytes = new byte[bb.limit() - 4];
			bb.get(unscaledbytes);

			return new BigDecimal(new BigInteger(unscaledbytes), scale);
		}
	};

	final VarLenPacker<InputStream> BLOB = new VarLenPacker<InputStream>() {

		@Override
		public void pack(ResizableBigByteBuffer buffer, InputStream value) {

			ByteBuffer byteBuffer = ByteBuffer.allocate(8192);
			try {
				for (ReadableByteChannel channel = Channels.newChannel(value); channel.read(byteBuffer) != -1;) {
					byteBuffer.flip();
					buffer.put(byteBuffer);
					byteBuffer.clear();
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public InputStream unpack(BigByteBuffer buffer) {
			return buffer.toInputStream();
		}
	};

	void pack(ResizableBigByteBuffer buffer, E value);

	E unpack(BigByteBuffer buffer);
}
