package tech.bitey.dataframe;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEnumColumn {

	private enum TestEnum {
		A, B, C, D, E
	}

	@Test
	public void basic() {

		EnumColumn<TestEnum> c1 = EnumColumn.wrap(TestEnum.class, ByteColumn.of((byte) 0, (byte) 2, (byte) 4));
		List<TestEnum> list = Arrays.asList(TestEnum.A, TestEnum.C, TestEnum.E);

		Assertions.assertEquals(TestEnum.A, c1.first());
		Assertions.assertEquals(TestEnum.E, c1.last());
		Assertions.assertEquals(list, c1);
		Assertions.assertEquals(c1, list);
		Assertions.assertTrue(c1.contains(TestEnum.C));
		Assertions.assertFalse(c1.contains(TestEnum.B));
		Assertions.assertFalse(c1.contains(TestEnum.D));
		Assertions.assertTrue(c1.containsAll(list));
		Assertions.assertTrue(list.containsAll(c1));

		Assertions.assertArrayEquals(list.toArray(), c1.toArray());
		Assertions.assertArrayEquals(list.toArray(new TestEnum[0]), c1.toArray(new TestEnum[0]));
		Assertions.assertArrayEquals(list.toArray(new TestEnum[list.size()]), c1.toArray(new TestEnum[c1.size()]));

		Assertions.assertEquals(list.subList(1, list.size() - 1), c1.subList(1, c1.size() - 1));
		Assertions.assertEquals(list.subList(1, list.size() - 1),
				c1.toDistinct().subColumnByValue(TestEnum.A, false, TestEnum.E, false));

		for (int i = 0; i < list.size(); i++) {
			Assertions.assertEquals(list.get(i), c1.get(i));
			Assertions.assertEquals(i, c1.indexOf(list.get(i)));
			Assertions.assertEquals(i, c1.lastIndexOf(list.get(i)));
		}
	}
}
