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

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.StringColumn;

abstract class TestColumn<E extends Comparable<E>> {

	private final E min, max;
	private final IntFunction<E[]> createArray;

	TestColumn(E min, E max, IntFunction<E[]> createArray) {
		this.min = min;
		this.max = max;
		this.createArray = createArray;
	}

	/*------------------------------------------------------------
	 *  Test Collection Methods
	 *------------------------------------------------------------*/
	@Test
	public void testSize() {
		for (TestSample<E> s : samples())
			Assertions.assertEquals(s.size(), s.column().size(), s + ", size");
	}

	@Test
	public void testIterator() {
		for (TestSample<E> s : samples()) {
			E[] array = s.array();
			Iterator<E> iter = s.column().iterator();
			for (int i = 0; i < array.length; i++) {
				Assertions.assertTrue(iter.hasNext(), s + ", iterator.hasNext, " + i);
				Assertions.assertEquals(array[i], iter.next(), s + ", iterator.next, " + i);
			}
			Assertions.assertFalse(iter.hasNext(), s + ", iterator.!hasNext");
		}
	}

	/*------------------------------------------------------------
	 *  Test List Methods
	 *------------------------------------------------------------*/
	@Test
	public void testGet() {
		for (TestSample<E> s : samples()) {
			E[] array = s.array();
			Column<E> column = s.column();
			for (int i = 0; i < array.length; i++)
				Assertions.assertEquals(array[i], column.get(i), s + ", get, " + i);
		}
	}

	@Test
	public void testIndexOf() {
		for (TestSample<E> s : samples()) {
			List<E> list = Arrays.asList(s.array());
			Column<E> column = s.column();
			for (E e : list) {
				int expected = list.indexOf(e);
				int actual = column.indexOf(e);
				Assertions.assertEquals(expected, actual, s + ", indexOf >= 0, " + e);
			}
			for (E e : notPresent()) {
				int actual = column.indexOf(e);
				Assertions.assertEquals(-1, actual, s + ", indexOf == -1, " + e);
			}
		}
	}

	@Test
	public void testLastIndexOf() {
		for (TestSample<E> s : samples()) {
			List<E> list = Arrays.asList(s.array());
			Column<E> column = s.column();
			for (E e : list) {
				int expected = list.lastIndexOf(e);
				int actual = column.lastIndexOf(e);
				Assertions.assertEquals(expected, actual, s + ", lastIndexOf >= 0, " + e);
			}
			for (E e : notPresent()) {
				int actual = column.lastIndexOf(e);
				Assertions.assertEquals(-1, actual, s + ", lastIndexOf == -1, " + e);
			}
		}
	}

	@Test
	public void testListIterator() {
		for (TestSample<E> s : samples()) {
			E[] array = s.array();
			ListIterator<E> iter = s.column().listIterator(array.length);

			// backwards
			for (int i = array.length - 1; i >= 0; i--) {
				Assertions.assertTrue(iter.hasPrevious(), s + ", listIterator.hasPrevious, " + i);
				Assertions.assertEquals(array[i], iter.previous(), s + ", listIterator.previous, " + i);
			}
			Assertions.assertFalse(iter.hasPrevious(), s + ", listIterator.!hasPrevious, ");

			// forward
			for (int i = 0; i < array.length; i++) {
				Assertions.assertTrue(iter.hasNext(), s + ", listIterator.hasNext, " + i);
				Assertions.assertEquals(array[i], iter.next(), s + ", listIterator.next, " + i);
			}
			Assertions.assertFalse(iter.hasNext(), s + ", listIterator.!hasNext");
		}
	}

	/*------------------------------------------------------------
	 *  Test Navigable Set Methods
	 *------------------------------------------------------------*/
	@Test
	public void testSubSet() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isDistinct() || s.size() < 2)
				continue;

			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();

			E first = set.first();
			E last = set.last();

			Assertions.assertEquals(new ArrayList<>(set.subSet(first, false, last, false)),
					column.subColumnByValue(first, false, last, false), s + ", testSubSetFF");

			Assertions.assertEquals(new ArrayList<>(set.subSet(first, true, last, false)),
					column.subColumnByValue(first, true, last, false), s + ", testSubSetTF");

			Assertions.assertEquals(new ArrayList<>(set.subSet(first, false, last, true)),
					column.subColumnByValue(first, false, last, true), s + ", testSubSetFT");

			Assertions.assertEquals(new ArrayList<>(set.subSet(first, true, last, true)),
					column.subColumnByValue(first, true, last, true), s + ", testSubSetTT");

			Assertions.assertEquals(new ArrayList<>(set.subSet(first, last)), column.subColumnByValue(first, last),
					s + ", testSubSet");
		}
	}

	@Test
	public void testTail() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isDistinct())
				continue;

			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();

			for (E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.tailSet(e)), column.tail(e), s + ", testTail, " + e);
		}
	}

	@Test
	public void testTailExclusive() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isDistinct())
				continue;

			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();

			for (E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.tailSet(e, false)), column.tail(e, false),
						s + ", testTailExclusive, " + e);
		}
	}

	@Test
	public void testHead() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isDistinct())
				continue;

			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();

			for (E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.headSet(e)), column.head(e), s + ", testHead, " + e);
		}
	}

	@Test
	public void testHeadInclusive() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isDistinct())
				continue;

			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();

			for (E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.headSet(e, true)), column.head(e, true),
						s + ", testHeadInclusive, " + e);
		}
	}

	@Test
	public void testAsSet() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isDistinct())
				continue;

			NavigableSet<E> expected = new TreeSet<>(asList(s.array()));
			NavigableSet<E> actual = s.column().asSet();

			Assertions.assertEquals(new ArrayList<>(expected), new ArrayList<>(actual), s + ", testAsSet, equals");
			Assertions.assertEquals(new ArrayList<>(expected.descendingSet()), new ArrayList<>(actual.descendingSet()),
					s + ", testAsSet, descending equals");

			if (expected.size() < 2)
				continue;

			E first = expected.first();
			E last = expected.last();

			Assertions.assertEquals(new ArrayList<>(expected.subSet(first, last)),
					new ArrayList<>(actual.subSet(first, last)), s + ", testAsSet, subSet1");
			Assertions.assertEquals(new ArrayList<>(expected.subSet(first, false, last, false)),
					new ArrayList<>(actual.subSet(first, false, last, false)), s + ", testAsSet, subSet2");

			Assertions.assertEquals(new ArrayList<>(expected.descendingSet().subSet(last, first)),
					new ArrayList<>(actual.descendingSet().subSet(last, first)), s + ", testAsSet, descending subSet1");
			Assertions.assertEquals(new ArrayList<>(expected.descendingSet().subSet(last, false, first, false)),
					new ArrayList<>(actual.descendingSet().subSet(last, false, first, false)),
					s + ", testAsSet, descending subSet2");
		}
	}

	/*------------------------------------------------------------
	 *  Test Characteristic Conversion Methods
	 *------------------------------------------------------------*/
	@Test
	public void testToHeap() {
		for (TestSample<E> s : samples()) {

			Column<E> heap = s.column().toHeap();
			Assertions.assertTrue(!s.column().isNonnull() || heap.isNonnull(), s + ", toHeap, nonnull");
			Assertions.assertFalse(heap.isSorted(), s + ", toHeap, sorted");
			Assertions.assertFalse(heap.isDistinct(), s + ", toHeap, distinct");

			if (!s.column().isSorted())
				Assertions.assertSame(heap, s.column(), s + ", toHeap, same");
			else {
				Assertions.assertNotSame(heap, s.column(), s + ", toHeap, not same");
				Assertions.assertEquals(heap, s.column(), s + ", toHeap, equals");
			}
		}
	}

	@Test
	public void testToSorted() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isNonnull())
				continue;

			Column<E> sorted = s.column().toSorted();

			Assertions.assertTrue(sorted.isNonnull(), s + ", toSorted, nonnull");
			Assertions.assertTrue(sorted.isSorted(), s + ", toSorted, sorted");
			Assertions.assertFalse(sorted.isDistinct(), s + ", toSorted, distinct");

			if (s.column().isSorted() && !s.column().isDistinct())
				Assertions.assertSame(s.column(), sorted, s + ", toSorted, same");
			else {
				Assertions.assertNotSame(s.column(), sorted, s + ", toSorted, not same");
				if (!s.column().isSorted()) {
					List<E> list = new ArrayList<>(s.column());
					Collections.sort(list);
					Assertions.assertEquals(list, sorted, s + ", toSorted, equals1");
				} else
					Assertions.assertEquals(s.column(), sorted, s + ", toSorted, equals2");
			}
		}
	}

	@Test
	public void testToDistinct() {
		for (TestSample<E> s : samples()) {
			if (!s.column().isNonnull())
				continue;

			Column<E> distinct = s.column().toDistinct();

			Assertions.assertTrue(distinct.isNonnull(), s + ", toDistinct, nonnull");
			Assertions.assertTrue(distinct.isSorted(), s + ", toDistinct, sorted");
			Assertions.assertTrue(distinct.isDistinct(), s + ", toDistinct, distinct");

			if (s.column().isDistinct())
				Assertions.assertSame(distinct, s.column(), s + ", toDistinct, same");
			else {
				List<E> expected = new ArrayList<>(new TreeSet<>(distinct));
				Assertions.assertEquals(expected, distinct, s + ", toDistinct, equals");
			}
		}
	}

	/*------------------------------------------------------------
	 *  Test Other Methods
	 *------------------------------------------------------------*/
	@Test
	public void testCopy() {
		for (TestSample<E> s : samples()) {
			List<E> list = asList(s.array());
			Column<E> copy = s.column().copy();

			Assertions.assertNotSame(s.column(), copy, s + ", testCopy, not same");
			Assertions.assertEquals(list, copy, s + ", testCopy, equals");
		}
	}

	@Test
	public void testAppendNoCoerce() {
		for (TestSample<E> s : samples()) {
			if (s.size() < 2)
				continue;

			int splitAt = s.column().size() / 2;
			Column<E> head = s.column().subColumn(0, splitAt);
			Column<E> tail = s.column().subColumn(splitAt, s.size());

			List<E> expected = asList(s.array());
			Column<E> actual = head.append(tail, false);

			Assertions.assertEquals(expected, actual, s + ", append(coerce==false), equals");
		}
	}

	/*------------------------------------------------------------
	 *  Test Column Conversion Methods
	 *------------------------------------------------------------*/
	@Test
	public void testColumnConversion() {

		for (TestSample<E> s : samples()) {

			Column<E> expected = s.column();

			StringColumn stringColumn = expected.toStringColumn();
			Column<E> actual = parseColumn(stringColumn);

			Assertions.assertEquals(expected, actual, s + ", column conversion, equals");
		}
	}

	abstract Column<E> parseColumn(StringColumn stringColumn);

	/*------------------------------------------------------------
	 *  Generate Samples
	 *------------------------------------------------------------*/
	List<TestSample<E>> samples() {

		List<TestSample<E>> baseSamples = baseSamples();
		Set<TestSample<E>> samples = new LinkedHashSet<>();

		samples.addAll(baseSamples);

		// reverse samples
		for (TestSample<E> s : new ArrayList<>(samples)) {
			if (s.size() <= 1)
				continue;

			E[] copy = s.copySample();
			Collections.reverse(asList(copy));
			TestSample<E> r = wrapSample(s.toString() + "_rev", copy);
			if (!samples.contains(r))
				samples.add(r);
		}

		// sorted samples
		for (TestSample<E> s : new ArrayList<>(samples)) {
			if (s.size() <= 1 || s.hasNull())
				continue;

			E[] copy = s.copySample();
			Arrays.sort(copy);

			TestSample<E> r = wrapSample(s.toString() + "_sorted", copy);
			if (!samples.contains(r))
				samples.add(r);
		}

		// sorted flag
		for (TestSample<E> s : new ArrayList<>(samples)) {
			if (s.hasNull())
				continue;

			E[] copy = s.copySample();
			Arrays.sort(copy);

			if (!Arrays.equals(s.array(), copy))
				continue;

			TestSample<E> r = wrapSample(s.toString() + "_sflag", copy, Spliterator.SORTED);
			if (!samples.contains(r))
				samples.add(r);
		}

		// distinct flag
		for (TestSample<E> s : new ArrayList<>(samples)) {
			if (s.hasNull())
				continue;

			List<E> distinct = new ArrayList<>(new HashSet<>(asList(s.copySample())));
			Collections.sort(distinct);

			TestSample<E> r = wrapSample(s.toString() + "_dflag", toArray(distinct), Spliterator.DISTINCT);
			if (!samples.contains(r))
				samples.add(r);
		}

		// sublist[1:size]
		for (TestSample<E> s : new ArrayList<>(samples)) {
			if (s.size() == 0 || s.size() > 1000)
				continue;

			TestSample<E> r = wrapSample(s, "[1:size]", 1, s.size());
			if (!samples.contains(r))
				samples.add(r);
		}

		// sublist[0:size-1]
		for (TestSample<E> s : new ArrayList<>(samples)) {
			if (s.size() == 0 || s.size() > 1000)
				continue;

			TestSample<E> r = wrapSample(s, "[0:size-1]", 0, s.size() - 1);
			if (!samples.contains(r))
				samples.add(r);
		}

		// sublist[1:size-1]
		for (TestSample<E> s : new ArrayList<>(samples)) {
			if (s.size() < 2 || s.size() > 1000)
				continue;

			TestSample<E> r = wrapSample(s, "[1:size-1]", 1, s.size() - 1);
			if (!samples.contains(r))
				samples.add(r);
		}

		return new ArrayList<>(samples);
	}

	List<TestSample<E>> baseSamples() {

		List<TestSample<E>> samples = new ArrayList<>();

		samples.add(wrapSample("empty", empty()));
		samples.add(wrapSample("singleNull", singleNull()));
		samples.add(wrapSample("singleNonNull", singleNonNull()));
		samples.add(wrapSample("duoFirstNull", duoFirstNull()));
		samples.add(wrapSample("duoBothNull", duoBothNull()));
		samples.add(wrapSample("duoSame", duoSame()));
		samples.add(wrapSample("minMax", minMax()));

		int[] sizes = { 7, 8, 9, 127, 128, 129, 1023, 1024, 1025 };
		for (int size : sizes) {
			if (this instanceof TestByteColumn && size > 127 || this instanceof TestNormalStringColumn && size > 256)
				break;

			samples.add(wrapSample("allNull_" + size, allNull(size)));
			samples.add(wrapSample("random_" + size, random(size)));
			samples.add(wrapSample("NXNX_" + size, NXNX(size)));
			samples.add(wrapSample("NNXX_" + size, NNXX(size)));
			samples.add(wrapSample("sequential_" + size, sequential(size)));
			samples.add(wrapSample("same_" + size, same(size)));
			samples.add(wrapSample("smar_" + size, smar(size)));
		}

		return samples;
	}

	abstract TestSample<E> wrapSample(String label, E[] array, int characteristics);

	abstract TestSample<E> wrapSample(String label, E[] array, Column<E> column, int fromIndex, int toIndex);

	TestSample<E> wrapSample(String label, E[] array) {
		return wrapSample(label, array, 0);
	}

	TestSample<E> wrapSample(TestSample<E> s, String labelSuffix, int fromIndex, int toIndex) {
		E[] array = toArray(asList(s.copySample()).subList(fromIndex, toIndex));
		Column<E> column = s.column().subColumn(fromIndex, toIndex);

		return wrapSample(s.toString() + labelSuffix, array, column, fromIndex, toIndex);
	}

	abstract E[] toArray(Collection<E> samples);

	abstract E[] notPresent();

	E[] empty() {
		return allNull(0);
	}

	E[] singleNull() {
		return allNull(1);
	}

	E[] singleNonNull() {
		E[] array = createArray.apply(1);
		array[0] = min;
		return array;
	}

	E[] duoFirstNull() {
		E[] array = createArray.apply(2);
		array[1] = min;
		return array;
	}

	E[] duoBothNull() {
		return allNull(2);
	}

	E[] duoSame() {
		E[] array = createArray.apply(2);
		array[0] = min;
		array[1] = min;
		return array;
	}

	E[] minMax() {
		E[] array = createArray.apply(2);
		array[0] = min;
		array[1] = max;
		return array;
	}

	E[] allNull(int size) {
		return createArray.apply(size);
	}

	abstract E[] random(int size);

	abstract E[] NXNX(int size);

	abstract E[] NNXX(int size);

	abstract E[] sequential(int size);

	abstract E[] same(int size);

	abstract E[] smar(int size);
}
