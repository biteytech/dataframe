package tech.bitey.bufferstuff;

sealed abstract class AbstractBigByteBuffer implements BigByteBuffer permits SimpleBigByteBuffer,CompoundBigByteBuffer {

	@Override
	public String toString() {
		return "[pos=%d lim=%d cap=%d]".formatted(position(), limit(), capacity());
	}
}
