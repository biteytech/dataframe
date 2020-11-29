package tech.bitey.dataframe;

import java.util.stream.Stream;

@FunctionalInterface
public interface GroupByReduction {

	Comparable<?> reduce(Stream<Row> rows);
}
