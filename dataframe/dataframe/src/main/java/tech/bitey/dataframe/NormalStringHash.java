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

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Used to represent a String as a key in a Map because a String can be very
 * large.
 * <p>
 * In theory there could be collisions, but in practice we're hashing with
 * SHA-256 so it's very very unlikely.
 */
class NormalStringHash {

	private static final ThreadLocal<MessageDigest> DIGEST = ThreadLocal.withInitial(() -> {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	});

	private final long l1, l2, l3, l4;

	NormalStringHash(String s) {

		LongBuffer buf = ByteBuffer.wrap(DIGEST.get().digest(s.getBytes())).asLongBuffer();

		l1 = buf.get();
		l2 = buf.get();
		l3 = buf.get();
		l4 = buf.get();
	}

	@Override
	public boolean equals(Object o) {
		NormalStringHash rhs = (NormalStringHash) o;
		return l1 == rhs.l1 && l2 == rhs.l2 && l3 == rhs.l3 && l4 == rhs.l4;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(l1 ^ l2 ^ l3 ^ l4);
	}
}
