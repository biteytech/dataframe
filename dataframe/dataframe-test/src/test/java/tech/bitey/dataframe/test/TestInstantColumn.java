package tech.bitey.dataframe.test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.InstantColumn;

public class TestInstantColumn {

	@Test
	public void now() {

		Instant expected = Instant.now();
		Instant actual = packUnpack(expected);

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void sort() {

		Instant now = Instant.now();

		List<Instant> expected = new ArrayList<>(List.of(now, now.plus(1, ChronoUnit.DAYS),
				now.minus(1000, ChronoUnit.DAYS), now.plusNanos(987654321), now.minusNanos(987654321)));
		InstantColumn actual = InstantColumn.of(expected);

		Collections.sort(expected);
		actual = actual.toSorted();

		Assertions.assertEquals(expected, actual);
	}

	private static Instant packUnpack(Instant date) {
		return InstantColumn.of(date).get(0);
	}
}
