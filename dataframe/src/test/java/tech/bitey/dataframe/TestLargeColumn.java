package tech.bitey.dataframe;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLargeColumn {

	@Test
	public void test() {

		Integer[] elements = new Integer[1 << 17];
		for (int i = 0; i < 1 << 16; i++)
			elements[i++] = i;

		IntColumn large = IntColumn.of(elements);

		Assertions.assertEquals(Arrays.asList(elements), large);
	}
}
