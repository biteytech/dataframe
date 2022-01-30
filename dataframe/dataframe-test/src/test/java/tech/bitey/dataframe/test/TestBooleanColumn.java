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

import java.util.Collection;
import java.util.stream.Stream;

import tech.bitey.dataframe.BooleanColumn;
import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.StringColumn;

public class TestBooleanColumn extends TestColumn<Boolean> {

	private TestIntColumn intColumn = new TestIntColumn();

	TestBooleanColumn() {
		super(Boolean.FALSE, Boolean.TRUE, Boolean[]::new);
	}

	@Override
	Column<Boolean> parseColumn(StringColumn stringColumn) {
		return stringColumn.parseBoolean();
	}

	@Override
	TestSample<Boolean> wrapSample(String label, Boolean[] array, int characteristics) {
		BooleanColumn column = BooleanColumn.builder().addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}

	@Override
	TestSample<Boolean> wrapSample(String label, Boolean[] array, Column<Boolean> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override
	public void testToSorted() {
	}

	@Override
	public void testToDistinct() {
	}

	@Override
	Boolean[] toArray(Collection<Boolean> samples) {
		return samples.toArray(empty());
	}

	@Override
	Column<Boolean> collect(Stream<Boolean> stream) {
		return stream.collect(BooleanColumn.collector());
	}

	Boolean[] fromInteger(Integer[] array) {
		Boolean[] bools = new Boolean[array.length];
		for (int i = 0; i < array.length; i++) {
			if (array[i] != null)
				bools[i] = array[i] % 2 == 0 ? Boolean.FALSE : Boolean.TRUE;
		}
		return bools;
	}

	@Override
	Boolean[] random(int size) {
		return fromInteger(intColumn.random(size));
	}

	@Override
	Boolean[] NXNX(int size) {
		return fromInteger(intColumn.NXNX(size));
	}

	@Override
	Boolean[] NNXX(int size) {
		return fromInteger(intColumn.NNXX(size));
	}

	@Override
	Boolean[] sequential(int size) {
		return fromInteger(intColumn.sequential(size));
	}

	@Override
	Boolean[] same(int size) {
		return fromInteger(intColumn.same(size));
	}

	@Override
	Boolean[] smar(int size) {
		return fromInteger(intColumn.smar(size));
	}

	@Override
	Boolean[] notPresent() {
		return new Boolean[] {};
	}
}
