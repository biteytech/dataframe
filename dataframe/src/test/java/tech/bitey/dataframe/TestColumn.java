package tech.bitey.dataframe;

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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

abstract class TestColumn<E extends Comparable<E>> {
	
	/*------------------------------------------------------------
	 *  Test Collection Methods
	 *------------------------------------------------------------*/	
	@Test
	public void testSize() {
		for(TestSample<E> s : samples())
			Assertions.assertEquals(s.size(), s.column().size(), s+", size");
	}
	
	@Test
	public void testIterator() {
		for(TestSample<E> s : samples()) {
			E[] array = s.array();			
			Iterator<E> iter = s.column().iterator();
			for(int i = 0; i < array.length; i++) {
				Assertions.assertTrue(iter.hasNext(), s+", iterator.hasNext, "+i);
				Assertions.assertEquals(array[i], iter.next(), s+", iterator.next, "+i);
			}
			Assertions.assertFalse(iter.hasNext(), s+", iterator.!hasNext");
		}
	}
	
	/*------------------------------------------------------------
	 *  Test List Methods
	 *------------------------------------------------------------*/
	@Test
	public void testGet() {
		for(TestSample<E> s : samples()) {
			E[] array = s.array();
			Column<E> column = s.column();
			for(int i = 0; i < array.length; i++)
				Assertions.assertEquals(array[i], column.get(i), s+", get, "+i);
		}
	}
	
	@Test
	public void testIndexOf() {
		for(TestSample<E> s : samples()) {
			List<E> list = Arrays.asList(s.array());
			Column<E> column = s.column();
			for(E e : list) {
				int expected = list.indexOf(e);
				int actual = column.indexOf(e);
				Assertions.assertEquals(expected, actual, s+", indexOf >= 0, "+e);
			}
			for(E e : notPresent()) {
				int actual = column.indexOf(e);
				Assertions.assertEquals(-1, actual, s+", indexOf == -1, "+e);
			}
		}
	}
	
	@Test
	public void testLastIndexOf() {
		for(TestSample<E> s : samples()) {
			List<E> list = Arrays.asList(s.array());
			Column<E> column = s.column();
			for(E e : list) {
				int expected = list.lastIndexOf(e);
				int actual = column.lastIndexOf(e);
				Assertions.assertEquals(expected, actual, s+", lastIndexOf >= 0, "+e);
			}
			for(E e : notPresent()) {
				int actual = column.lastIndexOf(e);
				Assertions.assertEquals(-1, actual, s+", lastIndexOf == -1, "+e);
			}
		}
	}
	
	@Test
	public void testListIterator() {
		for(TestSample<E> s : samples()) {
			E[] array = s.array();			
			ListIterator<E> iter = s.column().listIterator(array.length);
			
			// backwards
			for(int i = array.length-1; i >= 0; i--) {
				Assertions.assertTrue(iter.hasPrevious(), s+", listIterator.hasPrevious, "+i);
				Assertions.assertEquals(array[i], iter.previous(), s+", listIterator.previous, "+i);
			}
			Assertions.assertFalse(iter.hasPrevious(), s+", listIterator.!hasPrevious, ");
			
			// forward
			for(int i = 0; i < array.length; i++) {
				Assertions.assertTrue(iter.hasNext(), s+", listIterator.hasNext, "+i);
				Assertions.assertEquals(array[i], iter.next(), s+", listIterator.next, "+i);
			}
			Assertions.assertFalse(iter.hasNext(), s+", listIterator.!hasNext");
		}
	}
	
	/*------------------------------------------------------------
	 *  Test Navigable Set Methods
	 *------------------------------------------------------------*/
	@Test
	public void testSubSet() {
		for(TestSample<E> s : samples()) {
			if(!s.column().isDistinct() || s.size() < 2)
				continue;
			
			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();
			
			E first = set.first();
			E last = set.last();
			
			Assertions.assertEquals(new ArrayList<>(set.subSet(first, false, last, false)),
					column.subColumn(first, false, last, false), s+", testSubSet");
		}		
	}
	
	@Test
	public void testTail() {
		for(TestSample<E> s : samples()) {
			if(!s.column().isDistinct())
				continue;
			
			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();
			
			for(E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.tailSet(e)),
						column.tail(e), s+", testTail, "+e);
		}
	}	
	
	@Test
	public void testTailExclusive() {
		for(TestSample<E> s : samples()) {
			if(!s.column().isDistinct())
				continue;
			
			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();
			
			for(E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.tailSet(e, false)),
						column.tail(e, false), s+", testTailExclusive, "+e);
		}
	}
	
	@Test
	public void testHead() {
		for(TestSample<E> s : samples()) {
			if(!s.column().isDistinct())
				continue;
			
			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();
			
			for(E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.headSet(e)), column.head(e), s+", testHead, "+e);
		}
	}	
	
	@Test
	public void testHeadInclusive() {
		for(TestSample<E> s : samples()) {
			if(!s.column().isDistinct())
				continue;
			
			NavigableSet<E> set = new TreeSet<>(asList(s.array()));
			Column<E> column = s.column();
			
			for(E e : s.array())
				Assertions.assertEquals(new ArrayList<>(set.headSet(e, true)),
						column.head(e, true), s+", testHeadInclusive, "+e);
		}
	}
	
	/*------------------------------------------------------------
	 *  Test Characteristic Conversion Methods
	 *------------------------------------------------------------*/
	@Test
	public void testToHeap() {
		for(TestSample<E> s : samples()) {
			
			Column<E> heap = s.column().toHeap();
			Assertions.assertTrue(!s.column().isNonnull() || heap.isNonnull(), s+", toHeap, nonnull");
			Assertions.assertFalse(heap.isSorted(), s+", toHeap, sorted");
			Assertions.assertFalse(heap.isDistinct(), s+", toHeap, distinct");
			
			if(!s.column().isSorted())
				Assertions.assertSame(heap, s.column(), s+", toHeap, same");
			else {
				Assertions.assertNotSame(heap, s.column(), s+", toHeap, not same");
				Assertions.assertEquals(heap, s.column(), s+", toHeap, equals");
			}
		}
	}
	
	@Test
	public void testToSorted() {
		for(TestSample<E> s : samples()) {
			if(!s.column().isNonnull())
				continue;
			
			Column<E> sorted = s.column().toSorted();
			
			Assertions.assertTrue(sorted.isNonnull(), s+", toSorted, nonnull");
			Assertions.assertTrue(sorted.isSorted(), s+", toSorted, sorted");
			Assertions.assertFalse(sorted.isDistinct(), s+", toSorted, distinct");
			
			if(s.column().isSorted() && !s.column().isDistinct())
				Assertions.assertSame(s.column(), sorted, s+", toSorted, same");
			else {
				Assertions.assertNotSame(s.column(), sorted, s+", toSorted, not same");
				if(!s.column().isSorted()) {
					List<E> list = new ArrayList<>(s.column());
					Collections.sort(list);
					Assertions.assertEquals(list, sorted, s+", toSorted, equals1");
				}
				else
					Assertions.assertEquals(s.column(), sorted, s+", toSorted, equals2");
			}
		}
	}
	
	@Test
	public void testToDistinct() {
		for(TestSample<E> s : samples()) {
			if(!s.column().isNonnull())
				continue;
			
			Column<E> distinct = s.column().toDistinct();
			
			Assertions.assertTrue(distinct.isNonnull(), s+", toDistinct, nonnull");
			Assertions.assertTrue(distinct.isSorted(), s+", toDistinct, sorted");
			Assertions.assertTrue(distinct.isDistinct(), s+", toDistinct, distinct");
			
			if(s.column().isDistinct())
				Assertions.assertSame(distinct, s.column(), s+", toDistinct, same");
			else {
				List<E> expected = new ArrayList<>(new TreeSet<>(distinct));
				Assertions.assertEquals(expected, distinct, s+", toDistinct, equals");
			}				
		}
	}
	
	/*------------------------------------------------------------
	 *  Test Other Methods
	 *------------------------------------------------------------*/
	@Test
	public void testCopy() {
		for(TestSample<E> s : samples()) {
			List<E> list = asList(s.array());
			Column<E> copy = s.column().copy();
			
			Assertions.assertNotSame(s.column(), copy, s+", testCopy, not same");
			Assertions.assertEquals(list, copy, s+", testCopy, equals");
		}
	}
	
	@Test
	public void testAppendNoCoerce() {
		for(TestSample<E> s : samples()) {
			if(s.size() < 2)
				continue;
			
			int splitAt = s.column().size() / 2;
			Column<E> head = s.column().subColumn(0, splitAt);
			Column<E> tail = s.column().subColumn(splitAt, s.size());
			
			List<E> expected = asList(s.array());
			Column<E> actual = head.append(tail, false);
			
			Assertions.assertEquals(expected, actual, s+", append(coerce==false), equals");
		}
	}
	
	/*------------------------------------------------------------
	 *  Generate Samples
	 *------------------------------------------------------------*/
	List<TestSample<E>> samples() {
		
		List<TestSample<E>> baseSamples = baseSamples();
		Set<TestSample<E>> samples = new LinkedHashSet<>();
		
		samples.addAll(baseSamples);
		
		// reverse samples
		for(TestSample<E> s : new ArrayList<>(samples)) {
			if(s.size() <= 1)
				continue;
			
			E[] copy = s.copySample();
			Collections.reverse(asList(copy));
			TestSample<E> r = wrapSample(s.toString()+"_rev", copy);
			if(!samples.contains(r))
				samples.add(r);
		}
		
		// sorted samples
		for(TestSample<E> s : new ArrayList<>(samples)) {			 
			if(s.size() <= 1 || s.hasNull())
				continue;
			
			E[] copy = s.copySample();
			Arrays.sort(copy);
			
			TestSample<E> r = wrapSample(s.toString()+"_sorted", copy);
			if(!samples.contains(r))
				samples.add(r);
		}
		
		// sorted flag
		for(TestSample<E> s : new ArrayList<>(samples)) {			 
			if(s.hasNull())
				continue;
			
			E[] copy = s.copySample();
			Arrays.sort(copy);
			
			if(!Arrays.equals(s.array(), copy))
				continue;
			
			TestSample<E> r = wrapSample(s.toString()+"_sflag", copy, Spliterator.SORTED);
			if(!samples.contains(r))
				samples.add(r);
		}
		
		// distinct flag
		for(TestSample<E> s : new ArrayList<>(samples)) {			 
			if(s.hasNull())
				continue;
			
			List<E> distinct = new ArrayList<>(new HashSet<>(asList(s.copySample())));
			Collections.sort(distinct);			
			
			TestSample<E> r = wrapSample(s.toString()+"_dflag", toArray(distinct), Spliterator.DISTINCT);
			if(!samples.contains(r))
				samples.add(r);
		}
		
		// sublist[1:size]
		for(TestSample<E> s : new ArrayList<>(samples)) {			 
			if(s.size() == 0 || s.size() > 1000)
				continue;
			
			TestSample<E> r = wrapSample(s, "[1:size]", 1, s.size());
			if(!samples.contains(r))
				samples.add(r);
		}
		
		// sublist[0:size-1]
		for(TestSample<E> s : new ArrayList<>(samples)) {			 
			if(s.size() == 0 || s.size() > 1000)
				continue;
			
			TestSample<E> r = wrapSample(s, "[0:size-1]", 0, s.size()-1);
			if(!samples.contains(r))
				samples.add(r);
		}
		
		// sublist[1:size-1]
		for(TestSample<E> s : new ArrayList<>(samples)) {			 
			if(s.size() < 2 || s.size() > 1000)
				continue;
			
			TestSample<E> r = wrapSample(s, "[1:size-1]", 1, s.size()-1);
			if(!samples.contains(r))
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
		samples.add(wrapSample("duoDistinct", duoDistinct()));
		samples.add(wrapSample("duoSame", duoSame()));
		samples.add(wrapSample("minMax", minMax()));
		
		int[] sizes = {7, 8, 9, 127, 128, 129, 1023, 1024, 1025};
		for(int size : sizes) {
			samples.add(wrapSample("allNull_"+size, allNull(size)));
			samples.add(wrapSample("random_"+size, random(size)));
			samples.add(wrapSample("NXNX_"+size, NXNX(size)));
			samples.add(wrapSample("NNXX_"+size, NNXX(size)));
			samples.add(wrapSample("sequential_"+size, sequential(size)));
			samples.add(wrapSample("same_"+size, same(size)));
			samples.add(wrapSample("smar_"+size, smar(size)));
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
		
		return wrapSample(s.toString()+labelSuffix, array, column, fromIndex, toIndex);
	}
	
	abstract E[] toArray(Collection<E> samples);
	
	abstract E[] notPresent();
	
	// sample generators
	abstract E[] empty();
	
	abstract E[] singleNull();
	abstract E[] singleNonNull();
	
	abstract E[] duoFirstNull();
	abstract E[] duoBothNull();	
	abstract E[] duoDistinct();
	abstract E[] duoSame();
	abstract E[] minMax();
	
	abstract E[] allNull(int size);
	abstract E[] random(int size);
	abstract E[] NXNX(int size);
	abstract E[] NNXX(int size);
	abstract E[] sequential(int size);
	abstract E[] same(int size);
	abstract E[] smar(int size);
}
