/*
 * Copyright 2020 biteytech@protonmail.com
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

package tech.bitey.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TestByteColumn extends TestColumn<Byte> {

	private static final Random RAND = new Random(0);

	TestByteColumn() {
		super(Byte.MIN_VALUE, Byte.MAX_VALUE, Byte[]::new);
	}

	@Override
	Column<Byte> parseColumn(StringColumn stringColumn) {
		return stringColumn.parseByte();
	}

	@Override
	TestSample<Byte> wrapSample(String label, Byte[] array, int characteristics) {
		ByteColumn column = ByteColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}

	@Override
	TestSample<Byte> wrapSample(String label, Byte[] array, Column<Byte> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override
	Byte[] toArray(Collection<Byte> samples) {
		return samples.toArray(empty());
	}

	@Override
	Byte[] random(int size) {
		List<Byte> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new Byte[0]);
	}

	@Override
	Byte[] NXNX(int size) {
		Byte[] random = random(size);
		for (int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	Byte[] NNXX(int size) {
		Byte[] random = random(size);
		for (int i = 0; i < size; i += 4) {
			random[i] = null;
			if (i + 1 < size)
				random[i + 1] = null;
		}
		return random;
	}

	@Override
	Byte[] sequential(int size) {
		Byte[] elements = new Byte[size];
		for (int i = 0; i < size; i++)
			elements[i] = (byte) i;
		return elements;
	}

	@Override
	Byte[] same(int size) {
		Byte[] elements = new Byte[size];
		for (int i = 0; i < size; i++)
			elements[i] = 0;
		return elements;
	}

	@Override
	Byte[] smar(int size) {
		Byte[] elements = new Byte[size];
		byte n = 1;
		for (int i = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = n;
		return elements;
	}

	@Override
	Byte[] notPresent() {
		return new Byte[] { -77, -88, -99 };
	}

	// 1026 values
	private static final Byte[] RANDOM = new Byte[1026];
	static {
		for (int i = 0; i < 1024; i++)
			RANDOM[i] = Byte.valueOf((byte) (i & 0x7F));
		RANDOM[1024] = Byte.MIN_VALUE;
		RANDOM[1025] = Byte.MIN_VALUE;
	}
}
