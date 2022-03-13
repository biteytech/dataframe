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

public class GenBufferSpliterators implements GenBufferCode {

	@Override
	public void run() throws Exception {
		try (BufferedWriter out = open("BufferSpliterators.java")) {

			section(out, PREFIX);

			sections(out, false);
			sections(out, true);

			out.write("}\n");
		}
	}

	private void sections(BufferedWriter out, boolean small) throws Exception {

		String s = small ? "Small" : "";

		section(out, spliterator("Int", "Integer", s + "IntBuffer"));
		section(out, spliterator("Long", "Long", s + "LongBuffer"));
		section(out, spliterator("Double", "Double", s + "DoubleBuffer"));
	}

	private static String spliterator(String shortBoxType, String longBoxType, String bufferType) {
		return SPLITERATOR.replace(SHORT_BOX_TYPE, shortBoxType).replace(LONG_BOX_TYPE, longBoxType)
				.replace(BUFFER_TYPE, bufferType);
	}

	private static final String SHORT_BOX_TYPE = "SHORT_BOX_TYPE";
	private static final String LONG_BOX_TYPE = "LONG_BOX_TYPE";
	private static final String BUFFER_TYPE = "BUFFER_TYPE";

	private static final String SPLITERATOR = """
				/**
				 * A Spliterator.OfSHORT_BOX_TYPE designed for use by sources that traverse and split
				 * elements maintained in an unmodifiable {@link BUFFER_TYPE}.
				 * <p>
				 * Based on {@code Spliterators.SHORT_BOX_TYPEArraySpliterator}
				 */
				public static final class BUFFER_TYPESpliterator implements Spliterator.OfSHORT_BOX_TYPE {
					private final BUFFER_TYPE buffer;
					private int index; // current index, modified on advance/split
					private final int fence; // one past last index
					private final int characteristics;

					/**
					 * Creates a spliterator covering all of the given {@link BUFFER_TYPE}.
					 * <p>
					 * <b>Note:</b> ignores {@link BUFFER_TYPE#position() position} and
					 * {@link BUFFER_TYPE#limit() limit}, can pass a {@link BUFFER_TYPE#slice() slice}
					 * instead.
					 *
					 * @param buffer                    the buffer, assumed to be unmodified during
					 *                                  use
					 * @param additionalCharacteristics Additional spliterator characteristics of
					 *                                  this spliterator's source or elements beyond
					 *                                  {@code SIZED} and {@code SUBSIZED} which are
					 *                                  are always reported
					 */
					public BUFFER_TYPESpliterator(BUFFER_TYPE buffer, int additionalCharacteristics) {
						this(buffer, 0, buffer.capacity(), additionalCharacteristics);
					}

					/**
					 * Creates a spliterator covering the given {@link BUFFER_TYPE} and range.
					 *
					 * @param buffer                    the buffer, assumed to be unmodified during
					 *                                  use
					 * @param origin                    the least index (inclusive) to cover
					 * @param fence                     one past the greatest index to cover
					 * @param additionalCharacteristics Additional spliterator characteristics of
					 *                                  this spliterator's source or elements beyond
					 *                                  {@code SIZED} and {@code SUBSIZED} which are
					 *                                  are always reported
					 */
					public BUFFER_TYPESpliterator(BUFFER_TYPE buffer, int origin, int fence, int additionalCharacteristics) {
						this.buffer = buffer;
						this.index = origin;
						this.fence = fence;
						this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
					}

					@Override
					public OfSHORT_BOX_TYPE trySplit() {
						int lo = index, mid = (lo + fence) >>> 1;
						return (lo >= mid) ? null : new BUFFER_TYPESpliterator(buffer, lo, index = mid, characteristics);
					}

					@Override
					public void forEachRemaining(SHORT_BOX_TYPEConsumer action) {
						BUFFER_TYPE b;
						int i, hi; // hoist accesses and checks from loop
						if (action == null)
							throw new NullPointerException();
						if ((b = buffer).capacity() >= (hi = fence) && (i = index) >= 0 && i < (index = hi)) {
							do {
								action.accept(b.get(i));
							} while (++i < hi);
						}
					}

					@Override
					public boolean tryAdvance(SHORT_BOX_TYPEConsumer action) {
						if (action == null)
							throw new NullPointerException();
						if (index >= 0 && index < fence) {
							action.accept(buffer.get(index++));
							return true;
						}
						return false;
					}

					@Override
					public long estimateSize() {
						return fence - index;
					}

					@Override
					public int characteristics() {
						return characteristics;
					}

					@Override
					public Comparator<? super LONG_BOX_TYPE> getComparator() {
						if (hasCharacteristics(Spliterator.SORTED))
							return null;
						throw new IllegalStateException();
					}
				}
			""";

	private static final String PREFIX = """
			package tech.bitey.bufferstuff;

			import java.nio.DoubleBuffer;
			import java.nio.IntBuffer;
			import java.nio.LongBuffer;
			import java.util.Comparator;
			import java.util.Spliterator;
			import java.util.function.DoubleConsumer;
			import java.util.function.IntConsumer;
			import java.util.function.LongConsumer;

			public class BufferSpliterators {
			""";
}
