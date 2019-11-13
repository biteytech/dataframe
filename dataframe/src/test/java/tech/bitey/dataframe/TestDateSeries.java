package tech.bitey.dataframe;

import java.io.File;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDateSeries {

	@Test
	public void test1() throws Exception {

		DateSeriesBuilder builder = new DateSeriesBuilder();

		builder.add(20190101, 300);
		builder.add(2018, 1, 1, 200);
		builder.add(LocalDate.of(2017, 1, 1), 100);

		DateSeries ds1 = builder.build();

		DateSeries ds2 = ds1.transform(x -> x / 10).transform(ds1.transform(x -> 10), (a, b) -> a * b);
		Assertions.assertEquals(ds1, ds2);

		Map<LocalDate, Double> map = new HashMap<>();
		for (int i = 0; i < 3; i++)
			map.put(LocalDate.of(2017, 1, 1).plusYears(i), (i + 1) * 100d);
		Assertions.assertEquals(map, ds1.asMap());

		File file = File.createTempFile("ds1", "dat");
		file.deleteOnExit();
		ds1.writeTo(file);
		DateSeries ds3 = DateSeriesFactory.readFrom(file);
		Assertions.assertEquals(ds1, ds3);
	}
}
