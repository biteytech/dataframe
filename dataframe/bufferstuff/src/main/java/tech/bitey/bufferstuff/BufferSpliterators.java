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

	/**
	 * A Spliterator.OfInt designed for use by sources that traverse and split
	 * elements maintained in an unmodifiable {@link IntBuffer}.
	 * <p>
	 * Based on {@code Spliterators.IntArraySpliterator}
	 */
	public static final class IntBufferSpliterator implements Spliterator.OfInt {
		private final IntBuffer buffer;
		private int index; // current index, modified on advance/split
		private final int fence; // one past last index
		private final int characteristics;

		/**
		 * Creates a spliterator covering all of the given {@link IntBuffer}.
		 * <p>
		 * <b>Note:</b> ignores {@link IntBuffer#position() position} and
		 * {@link IntBuffer#limit() limit}, can pass a {@link IntBuffer#slice() slice}
		 * instead.
		 *
		 * @param buffer                    the buffer, assumed to be unmodified during
		 *                                  use
		 * @param additionalCharacteristics Additional spliterator characteristics of
		 *                                  this spliterator's source or elements beyond
		 *                                  {@code SIZED} and {@code SUBSIZED} which are
		 *                                  are always reported
		 */
		public IntBufferSpliterator(IntBuffer buffer, int additionalCharacteristics) {
			this(buffer, 0, buffer.capacity(), additionalCharacteristics);
		}

		/**
		 * Creates a spliterator covering the given {@link IntBuffer} and range.
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
		public IntBufferSpliterator(IntBuffer buffer, int origin, int fence, int additionalCharacteristics) {
			this.buffer = buffer;
			this.index = origin;
			this.fence = fence;
			this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
		}

		@Override
		public OfInt trySplit() {
			int lo = index, mid = (lo + fence) >>> 1;
			return (lo >= mid) ? null : new IntBufferSpliterator(buffer, lo, index = mid, characteristics);
		}

		@Override
		public void forEachRemaining(IntConsumer action) {
			IntBuffer b;
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
		public boolean tryAdvance(IntConsumer action) {
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
		public Comparator<? super Integer> getComparator() {
			if (hasCharacteristics(Spliterator.SORTED))
				return null;
			throw new IllegalStateException();
		}
	}

	/**
	 * A Spliterator.OfLong designed for use by sources that traverse and split
	 * elements maintained in an unmodifiable {@link LongBuffer}.
	 * <p>
	 * Based on {@code Spliterators.LongArraySpliterator}
	 */
	public static final class LongBufferSpliterator implements Spliterator.OfLong {
		private final LongBuffer buffer;
		private int index; // current index, modified on advance/split
		private final int fence; // one past last index
		private final int characteristics;

		/**
		 * Creates a spliterator covering all of the given {@link LongBuffer}.
		 * <p>
		 * <b>Note:</b> ignores {@link LongBuffer#position() position} and
		 * {@link LongBuffer#limit() limit}, can pass a {@link LongBuffer#slice() slice}
		 * instead.
		 *
		 * @param buffer                    the buffer, assumed to be unmodified during
		 *                                  use
		 * @param additionalCharacteristics Additional spliterator characteristics of
		 *                                  this spliterator's source or elements beyond
		 *                                  {@code SIZED} and {@code SUBSIZED} which are
		 *                                  are always reported
		 */
		public LongBufferSpliterator(LongBuffer buffer, int additionalCharacteristics) {
			this(buffer, 0, buffer.capacity(), additionalCharacteristics);
		}

		/**
		 * Creates a spliterator covering the given {@link LongBuffer} and range.
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
		public LongBufferSpliterator(LongBuffer buffer, int origin, int fence, int additionalCharacteristics) {
			this.buffer = buffer;
			this.index = origin;
			this.fence = fence;
			this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
		}

		@Override
		public OfLong trySplit() {
			int lo = index, mid = (lo + fence) >>> 1;
			return (lo >= mid) ? null : new LongBufferSpliterator(buffer, lo, index = mid, characteristics);
		}

		@Override
		public void forEachRemaining(LongConsumer action) {
			LongBuffer b;
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
		public boolean tryAdvance(LongConsumer action) {
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
		public Comparator<? super Long> getComparator() {
			if (hasCharacteristics(Spliterator.SORTED))
				return null;
			throw new IllegalStateException();
		}
	}

	/**
	 * A Spliterator.OfDouble designed for use by sources that traverse and split
	 * elements maintained in an unmodifiable {@link DoubleBuffer}.
	 * <p>
	 * Based on {@code Spliterators.DoubleArraySpliterator}
	 */
	public static final class DoubleBufferSpliterator implements Spliterator.OfDouble {
		private final DoubleBuffer buffer;
		private int index; // current index, modified on advance/split
		private final int fence; // one past last index
		private final int characteristics;

		/**
		 * Creates a spliterator covering all of the given {@link DoubleBuffer}.
		 * <p>
		 * <b>Note:</b> ignores {@link DoubleBuffer#position() position} and
		 * {@link DoubleBuffer#limit() limit}, can pass a {@link DoubleBuffer#slice()
		 * slice} instead.
		 *
		 * @param buffer                    the buffer, assumed to be unmodified during
		 *                                  use
		 * @param additionalCharacteristics Additional spliterator characteristics of
		 *                                  this spliterator's source or elements beyond
		 *                                  {@code SIZED} and {@code SUBSIZED} which are
		 *                                  are always reported
		 */
		public DoubleBufferSpliterator(DoubleBuffer buffer, int additionalCharacteristics) {
			this(buffer, 0, buffer.capacity(), additionalCharacteristics);
		}

		/**
		 * Creates a spliterator covering the given {@link DoubleBuffer} and range.
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
		public DoubleBufferSpliterator(DoubleBuffer buffer, int origin, int fence, int additionalCharacteristics) {
			this.buffer = buffer;
			this.index = origin;
			this.fence = fence;
			this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
		}

		@Override
		public OfDouble trySplit() {
			int lo = index, mid = (lo + fence) >>> 1;
			return (lo >= mid) ? null : new DoubleBufferSpliterator(buffer, lo, index = mid, characteristics);
		}

		@Override
		public void forEachRemaining(DoubleConsumer action) {
			DoubleBuffer b;
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
		public boolean tryAdvance(DoubleConsumer action) {
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
		public Comparator<? super Double> getComparator() {
			if (hasCharacteristics(Spliterator.SORTED))
				return null;
			throw new IllegalStateException();
		}
	}

	/**
	 * A Spliterator.OfInt designed for use by sources that traverse and split
	 * elements maintained in an unmodifiable {@link SmallIntBuffer}.
	 * <p>
	 * Based on {@code Spliterators.IntArraySpliterator}
	 */
	public static final class SmallIntBufferSpliterator implements Spliterator.OfInt {
		private final SmallIntBuffer buffer;
		private int index; // current index, modified on advance/split
		private final int fence; // one past last index
		private final int characteristics;

		/**
		 * Creates a spliterator covering all of the given {@link SmallIntBuffer}.
		 * <p>
		 * <b>Note:</b> ignores {@link SmallIntBuffer#position() position} and
		 * {@link SmallIntBuffer#limit() limit}, can pass a
		 * {@link SmallIntBuffer#slice() slice} instead.
		 *
		 * @param buffer                    the buffer, assumed to be unmodified during
		 *                                  use
		 * @param additionalCharacteristics Additional spliterator characteristics of
		 *                                  this spliterator's source or elements beyond
		 *                                  {@code SIZED} and {@code SUBSIZED} which are
		 *                                  are always reported
		 */
		public SmallIntBufferSpliterator(SmallIntBuffer buffer, int additionalCharacteristics) {
			this(buffer, 0, buffer.capacity(), additionalCharacteristics);
		}

		/**
		 * Creates a spliterator covering the given {@link SmallIntBuffer} and range.
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
		public SmallIntBufferSpliterator(SmallIntBuffer buffer, int origin, int fence, int additionalCharacteristics) {
			this.buffer = buffer;
			this.index = origin;
			this.fence = fence;
			this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
		}

		@Override
		public OfInt trySplit() {
			int lo = index, mid = (lo + fence) >>> 1;
			return (lo >= mid) ? null : new SmallIntBufferSpliterator(buffer, lo, index = mid, characteristics);
		}

		@Override
		public void forEachRemaining(IntConsumer action) {
			SmallIntBuffer b;
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
		public boolean tryAdvance(IntConsumer action) {
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
		public Comparator<? super Integer> getComparator() {
			if (hasCharacteristics(Spliterator.SORTED))
				return null;
			throw new IllegalStateException();
		}
	}

	/**
	 * A Spliterator.OfLong designed for use by sources that traverse and split
	 * elements maintained in an unmodifiable {@link SmallLongBuffer}.
	 * <p>
	 * Based on {@code Spliterators.LongArraySpliterator}
	 */
	public static final class SmallLongBufferSpliterator implements Spliterator.OfLong {
		private final SmallLongBuffer buffer;
		private int index; // current index, modified on advance/split
		private final int fence; // one past last index
		private final int characteristics;

		/**
		 * Creates a spliterator covering all of the given {@link SmallLongBuffer}.
		 * <p>
		 * <b>Note:</b> ignores {@link SmallLongBuffer#position() position} and
		 * {@link SmallLongBuffer#limit() limit}, can pass a
		 * {@link SmallLongBuffer#slice() slice} instead.
		 *
		 * @param buffer                    the buffer, assumed to be unmodified during
		 *                                  use
		 * @param additionalCharacteristics Additional spliterator characteristics of
		 *                                  this spliterator's source or elements beyond
		 *                                  {@code SIZED} and {@code SUBSIZED} which are
		 *                                  are always reported
		 */
		public SmallLongBufferSpliterator(SmallLongBuffer buffer, int additionalCharacteristics) {
			this(buffer, 0, buffer.capacity(), additionalCharacteristics);
		}

		/**
		 * Creates a spliterator covering the given {@link SmallLongBuffer} and range.
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
		public SmallLongBufferSpliterator(SmallLongBuffer buffer, int origin, int fence,
				int additionalCharacteristics) {
			this.buffer = buffer;
			this.index = origin;
			this.fence = fence;
			this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
		}

		@Override
		public OfLong trySplit() {
			int lo = index, mid = (lo + fence) >>> 1;
			return (lo >= mid) ? null : new SmallLongBufferSpliterator(buffer, lo, index = mid, characteristics);
		}

		@Override
		public void forEachRemaining(LongConsumer action) {
			SmallLongBuffer b;
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
		public boolean tryAdvance(LongConsumer action) {
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
		public Comparator<? super Long> getComparator() {
			if (hasCharacteristics(Spliterator.SORTED))
				return null;
			throw new IllegalStateException();
		}
	}

	/**
	 * A Spliterator.OfDouble designed for use by sources that traverse and split
	 * elements maintained in an unmodifiable {@link SmallDoubleBuffer}.
	 * <p>
	 * Based on {@code Spliterators.DoubleArraySpliterator}
	 */
	public static final class SmallDoubleBufferSpliterator implements Spliterator.OfDouble {
		private final SmallDoubleBuffer buffer;
		private int index; // current index, modified on advance/split
		private final int fence; // one past last index
		private final int characteristics;

		/**
		 * Creates a spliterator covering all of the given {@link SmallDoubleBuffer}.
		 * <p>
		 * <b>Note:</b> ignores {@link SmallDoubleBuffer#position() position} and
		 * {@link SmallDoubleBuffer#limit() limit}, can pass a
		 * {@link SmallDoubleBuffer#slice() slice} instead.
		 *
		 * @param buffer                    the buffer, assumed to be unmodified during
		 *                                  use
		 * @param additionalCharacteristics Additional spliterator characteristics of
		 *                                  this spliterator's source or elements beyond
		 *                                  {@code SIZED} and {@code SUBSIZED} which are
		 *                                  are always reported
		 */
		public SmallDoubleBufferSpliterator(SmallDoubleBuffer buffer, int additionalCharacteristics) {
			this(buffer, 0, buffer.capacity(), additionalCharacteristics);
		}

		/**
		 * Creates a spliterator covering the given {@link SmallDoubleBuffer} and range.
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
		public SmallDoubleBufferSpliterator(SmallDoubleBuffer buffer, int origin, int fence,
				int additionalCharacteristics) {
			this.buffer = buffer;
			this.index = origin;
			this.fence = fence;
			this.characteristics = additionalCharacteristics | Spliterator.SIZED | Spliterator.SUBSIZED;
		}

		@Override
		public OfDouble trySplit() {
			int lo = index, mid = (lo + fence) >>> 1;
			return (lo >= mid) ? null : new SmallDoubleBufferSpliterator(buffer, lo, index = mid, characteristics);
		}

		@Override
		public void forEachRemaining(DoubleConsumer action) {
			SmallDoubleBuffer b;
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
		public boolean tryAdvance(DoubleConsumer action) {
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
		public Comparator<? super Double> getComparator() {
			if (hasCharacteristics(Spliterator.SORTED))
				return null;
			throw new IllegalStateException();
		}
	}

}
