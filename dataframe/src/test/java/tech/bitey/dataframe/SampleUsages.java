package tech.bitey.dataframe;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SampleUsages {

	/**
	 * Creating {@link Column columns}.
	 */
	@Test
	public void ex0() {

		IntColumn c1 = IntColumn.of(1, 2, null, 3, null, null, 5);
		IntColumn c2 = IntColumn.builder().addAll(1, 2).addNull().add(3).addNulls(2).add(5).build();

		Assertions.assertEquals(c1, c2);
	}

	/**
	 * Converting arrays and collections to {@link Column columns}.
	 */
	@Test
	public void ex1() {
		// primitive arrays
		{
			int[] a = { 5, 1, 3 };

			IntColumn c = IntColumn.builder().addAll(a).build();

			Assertions.assertEquals(a[1], c.getInt(1));
			Assertions.assertArrayEquals(a, c.intStream().toArray());
		}

		// Object arrays
		{
			Integer[] a = { 5, 1, 3 };
			String[] b = { "e", "a", "c" };

			IntColumn c1 = IntColumn.of(a);
			StringColumn c2 = StringColumn.of(b);

			Assertions.assertEquals(a[1], c1.get(1));
			Assertions.assertEquals(b[1], c2.get(1));

			Assertions.assertArrayEquals(a, c1.toArray());
			Assertions.assertArrayEquals(b, c2.toArray());
		}

		// Collections
		{
			List<Integer> a = Arrays.asList(new Integer[] { 5, 1, 3 });
			List<String> b = Arrays.asList(new String[] { "e", "a", "c" });

			IntColumn c1 = IntColumn.of(a);
			StringColumn c2 = StringColumn.of(b);

			Assertions.assertEquals(a.get(1), c1.get(1));
			Assertions.assertEquals(b.get(1), c2.get(1));

			Assertions.assertEquals(a, c1);
			Assertions.assertEquals(b, c2);
		}
	}

	/**
	 * Sorting {@link Column columns}.
	 */
	@Test
	public void ex2() {

		List<String> l = Arrays.asList(new String[] { "c", "b", "a", "c" });
		StringColumn s = StringColumn.of(l);

		Assertions.assertEquals(l, s);

		Collections.sort(l);
		Assertions.assertEquals(l, s.toSorted());
		Assertions.assertEquals(l.subList(0, 3), s.toDistinct());
	}

	/**
	 * {@link Column Column} conversions.
	 */
	public void ex3() {

		StringColumn s = StringColumn.of("1", "2", null, "3");

		Assertions.assertEquals(s, s.parseInt().toStringColumn());
		Assertions.assertEquals(s, s.toIntColumn(Integer::parseInt).toStringColumn(Object::toString));
	}
}
