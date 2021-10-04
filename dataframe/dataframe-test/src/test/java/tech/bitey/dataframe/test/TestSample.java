/*
 * Copyright 2021 biteytech@protonmail.com
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

package tech.bitey.dataframe.test;

import java.util.Arrays;
import java.util.Objects;

import tech.bitey.dataframe.Column;

public class TestSample<E extends Comparable<? super E>> {

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
