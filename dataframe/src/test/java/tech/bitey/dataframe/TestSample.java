package tech.bitey.dataframe;

import java.util.Arrays;
import java.util.Objects;

public class TestSample<E> {

	private final String label;
	private final E[] array;
	private final Column<E> column;
	private final int fromIndex;
	private final int toIndex;

	private final boolean hasNull;

	TestSample(String label, E[] array, int fromIndex, int toIndex, Column<E> column) {
		this.label = label;
		this.array = array;
		this.column = column;
		this.fromIndex = fromIndex;
		this.toIndex = toIndex;

		boolean hasNull = false;
		for (E e : array) {
			if (e == null) {
				hasNull = true;
				break;
			}
		}
		this.hasNull = hasNull;
	}

	E[] array() {
		return array;
	}

	Column<E> column() {
		return column;
	}

	int size() {
		return array.length;
	}

	E[] copySample() {
		return Arrays.copyOf(array, array.length);
	}

	boolean hasNull() {
		return hasNull;
	}

	@Override
	public String toString() {
		return label;
	}

	@Override
	public int hashCode() {
		return Objects.hash(fromIndex, toIndex, column.characteristics(), Arrays.hashCode(array));
	}

	@Override
	public boolean equals(Object o) {
		@SuppressWarnings("rawtypes")
		TestSample rhs = (TestSample) o;

		return fromIndex == rhs.fromIndex && toIndex == rhs.toIndex
				&& column.characteristics() == rhs.column.characteristics() && Arrays.equals(array, rhs.array);
	}
}
