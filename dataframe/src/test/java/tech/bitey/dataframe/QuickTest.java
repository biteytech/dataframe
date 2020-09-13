package tech.bitey.dataframe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;

public class QuickTest {

	public static void main(String[] args) {
		TreeSet<Integer> test = new TreeSet<>();
		for (int i = 1; i <= 5; i++)
			test.add(i);

		Collection<Integer> o1, o2;

		System.out.println(test);
		System.out.println(test.subSet(2, true, 4, true));
		System.out.println(test.descendingSet());
		System.out.println(o1 = test.descendingSet().subSet(4, true, 2, true));

		System.out.println();

		NavigableSet<Integer> test2 = IntColumn.of(1, 2, 3, 4, 5).toDistinct().asSet();
		System.out.println(test2);
		System.out.println(test2.subSet(2, true, 4, true));
		System.out.println(test2.descendingSet());
		System.out.println(o2 = test2.descendingSet().subSet(4, true, 2, true));

		System.out.println(new ArrayList<>(o1).equals(new ArrayList<>(o2)));
	}

}
