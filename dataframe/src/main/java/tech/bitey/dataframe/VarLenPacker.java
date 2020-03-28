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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

interface VarLenPacker<E> {

	final VarLenPacker<String> STRING = new VarLenPacker<String>() {
		@Override
		public byte[] pack(String value) {
			return value.getBytes(UTF_8);
		}

		@Override
		public String unpack(byte[] packed) {
			return new String(packed, UTF_8);
		}
	};

	final VarLenPacker<BigDecimal> DECIMAL = new VarLenPacker<BigDecimal>() {
		@Override
		public byte[] pack(BigDecimal value) {

			byte[] unscaledBytes = value.unscaledValue().toByteArray();

			ByteBuffer packed = ByteBuffer.allocate(4 + unscaledBytes.length).order(BIG_ENDIAN);

			packed.putInt(value.scale());
			packed.put(unscaledBytes);

			return packed.array();
		}

		@Override
		public BigDecimal unpack(byte[] packed) {

			ByteBuffer buffer = ByteBuffer.wrap(packed).order(BIG_ENDIAN);

			int scale = buffer.getInt();

			byte[] unscaledbytes = new byte[packed.length - 4];
			buffer.get(unscaledbytes);

			return new BigDecimal(new BigInteger(unscaledbytes), scale);
		}
	};

	byte[] pack(E value);

	E unpack(byte[] packed);
}
