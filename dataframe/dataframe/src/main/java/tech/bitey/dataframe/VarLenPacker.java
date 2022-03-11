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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

interface VarLenPacker<E> {

	final VarLenPacker<String> STRING_UTF_8 = new VarLenPacker<String>() {
		@Override
		public ByteBuffer pack(String value) {
			return UTF_8.encode(value);
		}

		@Override
		public String unpack(ByteBuffer buffer) {
			return UTF_8.decode(buffer).toString();
		}
	};

	final VarLenPacker<String> STRING_ASCII = new VarLenPacker<String>() {
		@Override
		public ByteBuffer pack(String value) {
			return US_ASCII.encode(value);
		}

		@Override
		public String unpack(ByteBuffer buffer) {
			return US_ASCII.decode(buffer).toString();
		}
	};

	final VarLenPacker<BigDecimal> DECIMAL = new VarLenPacker<BigDecimal>() {
		@Override
		public ByteBuffer pack(BigDecimal value) {

			byte[] unscaledBytes = value.unscaledValue().toByteArray();

			ByteBuffer packed = ByteBuffer.allocate(4 + unscaledBytes.length).order(BIG_ENDIAN);

			packed.putInt(value.scale());
			packed.put(unscaledBytes);

			return packed.flip();
		}

		@Override
		public BigDecimal unpack(ByteBuffer buffer) {

			buffer = buffer.order(BIG_ENDIAN);

			int scale = buffer.getInt();

			byte[] unscaledbytes = new byte[buffer.limit() - 4];
			buffer.get(unscaledbytes);

			return new BigDecimal(new BigInteger(unscaledbytes), scale);
		}
	};

	ByteBuffer pack(E value);

	E unpack(ByteBuffer buffer);
}
