package tech.bitey.bufferstuff;

import static tech.bitey.bufferstuff.CompoundBigByteBuffer.CHUNK_MASK;

import java.nio.ByteBuffer;

public class ResizableBigByteBuffer {

	private BigByteBuffer buffer = BufferUtils.allocateBig(8);

	public long size() {
		return buffer.position();
	}

	public void addAll(ByteBuffer buffer) {

		ensureAdditionalCapacity(buffer.remaining());

		this.buffer.put(buffer);
	}

	public void append(ResizableBigByteBuffer tail) {
		if (tail.size() == 0)
			return;

		ensureAdditionalCapacity(tail.size());

		buffer.put(tail.buffer.duplicate().flip());
	}

	private void ensureAdditionalCapacity(long additionalCapacity) {
		ensureCapacity(size() + additionalCapacity);
	}

	private void ensureCapacity(long minCapacity) {
		if (buffer.capacity() < minCapacity) {

			long newCapacity = buffer.capacity() + (buffer.capacity() >> 1) + 1;
			if (newCapacity < minCapacity)
				newCapacity = minCapacity;

			// TODO: make this more efficient (avoid copying everything)
			BigByteBuffer extended = BufferUtils.allocateBig(newCapacity);
			buffer.flip();
			extended.put(buffer);
			buffer = extended;
		}
	}

	public BigByteBuffer trim() {

		if (size() == 0)
			return BufferUtils.EMPTY_BIG_BUFFER;

		ByteBuffer[] buffers = buffer.buffers();

		ByteBuffer last = buffers[buffers.length - 1];
		int rem = (int) (size() & CHUNK_MASK);
		if (rem != 0)
			buffers[buffers.length - 1] = BufferUtils.copy(last, 0, rem);

		return BufferUtils.wrap(buffers);
	}

	@Override
	public String toString() {
		return "[pos=%d lim=%d cap=%d]".formatted(buffer.position(), buffer.limit(), buffer.capacity());
	}
}
