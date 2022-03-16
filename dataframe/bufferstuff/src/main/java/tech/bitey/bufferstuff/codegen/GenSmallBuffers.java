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

package tech.bitey.bufferstuff.codegen;

import java.io.BufferedWriter;

public class GenSmallBuffers implements GenBufferCode {

	@Override
	public void run() throws Exception {
		run("Byte", "byte", 0);
		run("Short", "short", 1);
		run("Int", "int", 2);
		run("Long", "long", 3);
		run("Float", "float", 2);
		run("Double", "double", 3);
	}

	private void run(String bufferType, String valType, int shift) throws Exception {
		try (BufferedWriter out = open("Small" + bufferType + "Buffer.java")) {
			out.write(FILE.replace(BUFFER_TYPE, bufferType).replace(VAL_TYPE, valType).replace(SHIFT_BITS, "" + shift)
					.replace("putByte", "put").replace("getByte", "get"));
		}
	}

	private static final String BUFFER_TYPE = "BUFFER_TYPE";
	private static final String VAL_TYPE = "VAL_TYPE";
	private static final String SHIFT_BITS = "SHIFT_BITS";

	private static final String FILE = """
			package tech.bitey.bufferstuff;

			import java.nio.BufferOverflowException;
			import java.nio.BufferUnderflowException;
			import java.nio.BUFFER_TYPEBuffer;

			/**
			 * This class has an API similar to {@link BUFFER_TYPEBuffer}. All implementations are
			 * backed by a {@link BigByteBuffer}. "Small" refers to the fact that these
			 * buffers are indexed by ints rather than longs.
			 * <p>
			 * Differences from {@code BUFFER_TYPEBuffer} include:
			 * <ul>
			 * <li>mark and reset are not supported
			 * <li>read-only is not supported
			 * <li>byte order is preserved in {@link #duplicate()} and {@link #slice()}.
			 * </ul>
			 */
			public final class SmallBUFFER_TYPEBuffer extends SmallBuffer {

				private static final int SHIFT = SHIFT_BITS;

				SmallBUFFER_TYPEBuffer(BigByteBuffer buffer) {
					super(buffer);
				}

				/**
				 * Relative <i>put</i> method
				 * <p>
				 * Writes the given VAL_TYPE into this buffer at the current position, and then
				 * increments the position.
				 *
				 * @param value The VAL_TYPE to be written
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If this buffer's current position is not
				 *                                 smaller than its limit
				 */
				public SmallBUFFER_TYPEBuffer put(VAL_TYPE value) {
					buffer.putBUFFER_TYPE(value);
					return this;
				}

				/**
				 * Absolute <i>put</i> method
				 * <p>
				 * Writes the given VAL_TYPE into this buffer at the given index.
				 *
				 * @param index The index at which the VAL_TYPE will be written
				 *
				 * @param value The VAL_TYPE value to be written
				 *
				 * @return This buffer
				 *
				 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
				 *                                   than the buffer's limit
				 */
				public SmallBUFFER_TYPEBuffer put(int index, VAL_TYPE value) {
					buffer.putBUFFER_TYPE((long) index << SHIFT, value);
					return this;
				}

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the entire content of the given source BUFFER_TYPE array into
				 * this buffer.
				 *
				 * @param src The source array
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there is insufficient space in this buffer
				 */
				public final SmallBUFFER_TYPEBuffer put(VAL_TYPE[] src) {
					buffer.putBUFFER_TYPE(src);
					return this;
				}

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the VAL_TYPEs remaining in the given source buffer into
				 * this buffer. If there are more VAL_TYPEs remaining in the source buffer than in
				 * this buffer, that is, if
				 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
				 * shorts are transferred and a {@link BufferOverflowException} is thrown.
				 * <p>
				 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
				 * VAL_TYPEs from the given buffer into this buffer, starting at each buffer's
				 * current position. The positions of both buffers are then incremented by
				 * <i>n</i>.
				 *
				 * <p>
				 * In other words, an invocation of this method of the form {@code dst.put(src)}
				 * has exactly the same effect as the loop
				 *
				 * <pre>
				 * while (src.hasRemaining())
				 * 	dst.put(src.get());
				 * </pre>
				 *
				 * except that it first checks that there is sufficient space in this buffer and
				 * it is potentially much more efficient. If this buffer and the source buffer
				 * share the same backing array or memory, then the result will be as if the
				 * source elements were first copied to an intermediate location before being
				 * written into this buffer.
				 *
				 * @param src The source buffer from which VAL_TYPEs are to be read; must not be
				 *            this buffer
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there is insufficient space in this buffer
				 *                                 for the remaining VAL_TYPEs in the source buffer
				 */
				public SmallBUFFER_TYPEBuffer put(BUFFER_TYPEBuffer src) {
					buffer.putBUFFER_TYPE(src);
					return this;
				}

				/**
				 * Relative bulk <i>put</i> method
				 * <p>
				 * This method transfers the VAL_TYPEs remaining in the given source buffer into
				 * this buffer. If there are more VAL_TYPEs remaining in the source buffer than in
				 * this buffer, that is, if
				 * {@code src.remaining()}&nbsp;{@code >}&nbsp;{@code remaining()}, then no
				 * shorts are transferred and a {@link BufferOverflowException} is thrown.
				 * <p>
				 * Otherwise, this method copies <i>n</i>&nbsp;=&nbsp;{@code src.remaining()}
				 * VAL_TYPEs from the given buffer into this buffer, starting at each buffer's
				 * current position. The positions of both buffers are then incremented by
				 * <i>n</i>.
				 *
				 * <p>
				 * In other words, an invocation of this method of the form {@code dst.put(src)}
				 * has exactly the same effect as the loop
				 *
				 * <pre>
				 * while (src.hasRemaining())
				 * 	dst.put(src.get());
				 * </pre>
				 *
				 * except that it first checks that there is sufficient space in this buffer and
				 * it is potentially much more efficient. If this buffer and the source buffer
				 * share the same backing array or memory, then the result will be as if the
				 * source elements were first copied to an intermediate location before being
				 * written into this buffer.
				 *
				 * @param src The source buffer from which VAL_TYPEs are to be read; must not be
				 *            this buffer
				 *
				 * @return This buffer
				 *
				 * @throws BufferOverflowException If there is insufficient space in this buffer
				 *                                 for the remaining VAL_TYPEs in the source buffer
				 */
				public SmallBUFFER_TYPEBuffer put(SmallBUFFER_TYPEBuffer src) {
					buffer.put(src.buffer);
					return this;
				}

				/**
				 * Relative <i>get</i> method. Reads the VAL_TYPE at this buffer's current
				 * position, and then increments the position.
				 *
				 * @return The VAL_TYPE at the buffer's current position
				 *
				 * @throws BufferUnderflowException If the buffer's current position is not
				 *                                  smaller than its limit
				 */
				public VAL_TYPE get() {
					return buffer.getBUFFER_TYPE();
				}

				/**
				 * Absolute <i>get</i> method. Reads the VAL_TYPE at the given index.
				 *
				 * @param index The index from which the VAL_TYPE will be read
				 *
				 * @return The VAL_TYPE at the given index
				 *
				 * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller
				 *                                   than the buffer's limit
				 */
				public VAL_TYPE get(int index) {
					return buffer.getBUFFER_TYPE((long) index << SHIFT);
				}

				@Override
				int shift() {
					return SHIFT;
				}

				@Override
				public SmallBUFFER_TYPEBuffer duplicate() {
					return new SmallBUFFER_TYPEBuffer(buffer.duplicate());
				}

				@Override
				public SmallBUFFER_TYPEBuffer slice() {
					return new SmallBUFFER_TYPEBuffer(buffer.slice());
				}

				@Override
				public String toString() {
					return "[pos=%d lim=%d cap=%d]".formatted(buffer.position(), buffer.limit(), buffer.capacity());
				}

			}
			""";
}
