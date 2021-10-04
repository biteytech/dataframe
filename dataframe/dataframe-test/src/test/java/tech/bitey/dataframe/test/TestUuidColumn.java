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
import java.util.UUID;

import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.StringColumn;
import tech.bitey.dataframe.UuidColumn;

public class TestUuidColumn extends TestColumn<UUID> {

	private final TestLongColumn longCol = new TestLongColumn();

	TestUuidColumn() {
		super(wrap(Long.MIN_VALUE), wrap(Long.MAX_VALUE), UUID[]::new);
	}

	@Override
	Column<UUID> parseColumn(StringColumn stringColumn) {
		return stringColumn.parseUuid();
	}

	@Override
	TestSample<UUID> wrapSample(String label, UUID[] array, int characteristics) {
		UuidColumn column = UuidColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}

	@Override
	TestSample<UUID> wrapSample(String label, UUID[] array, Column<UUID> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override
	UUID[] toArray(Collection<UUID> samples) {
		return samples.toArray(empty());
	}

	@Override
	UUID[] notPresent() {
		return wrap(longCol.notPresent());
	}

	@Override
	UUID[] random(int size) {
		return wrap(longCol.random(size));
	}

	@Override
	UUID[] NXNX(int size) {
		return wrap(longCol.NXNX(size));
	}

	@Override
	UUID[] NNXX(int size) {
		return wrap(longCol.NNXX(size));
	}

	@Override
	UUID[] sequential(int size) {
		return wrap(longCol.sequential(size));
	}

	@Override
	UUID[] same(int size) {
		return wrap(longCol.same(size));
	}

	@Override
	UUID[] smar(int size) {
		return wrap(longCol.smar(size));
	}

	private static UUID[] wrap(Long[] longs) {
		UUID[] uuids = new UUID[longs.length];
		for (int i = 0; i < longs.length; i++)
			uuids[i] = wrap(longs[i]);
		return uuids;
	}

	private static UUID wrap(Long l) {
		return l == null ? null : new UUID(l, l);
	}
}
