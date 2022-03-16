package tech.bitey.bufferstuff;

abstract class AbstractBigByteBuffer implements BigByteBuffer {

	@Override
	public String toString() {
		return "[pos=%d lim=%d cap=%d]".formatted(position(), limit(), capacity());
	}
}
