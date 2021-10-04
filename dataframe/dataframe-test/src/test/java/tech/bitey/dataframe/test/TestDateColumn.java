package tech.bitey.dataframe.test;

import java.time.LocalDate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.DateColumn;

public class TestDateColumn {

	@Test
	public void now() {

		LocalDate expected = LocalDate.now();
		LocalDate actual = packUnpack(expected);

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void largeYears() {

		int[] years = { -(1 << 22), -99999, 99999, (1 << 22) - 1 };

		for (int year : years) {

			LocalDate expected = LocalDate.of(year, 1, 1);
			LocalDate actual = packUnpack(expected);

			Assertions.assertEquals(expected, actual);
		}
	}

	private static LocalDate packUnpack(LocalDate date) {
		return DateColumn.of(date).get(0);
	}
}
