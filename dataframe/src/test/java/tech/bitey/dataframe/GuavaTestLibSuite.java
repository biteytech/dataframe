package tech.bitey.dataframe;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.IntFunction;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.testing.ListTestSuiteBuilder;
import com.google.common.collect.testing.NavigableSetTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestListGenerator;
import com.google.common.collect.testing.TestSortedSetGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.testers.ListEqualsTester;
import com.google.common.collect.testing.testers.ListHashCodeTester;

import junit.framework.TestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ GuavaTestLibSuite.ColumnTests.class })
public class GuavaTestLibSuite {

	private static final Method LIST_TEST_HASH_CODE = suppressMethod(ListHashCodeTester.class, "testHashCode");
	private static final Method LIST_TEST_EQUALS_SET = suppressMethod(ListEqualsTester.class, "testEquals_set");

	public static class ColumnTests {
		public static TestSuite suite() {

			TestSuite suite = new TestSuite("ColumnTests");

			addTests(suite, ColumnType.STRING, String[]::new, "!! a", "!! b", "b", "a", "c", "d", "e", "~~ a", "~~ b");

			addTests(suite, ColumnType.BYTE, Byte[]::new, Byte.MIN_VALUE, (byte) 0, (byte) 2, (byte) 1, (byte) 3,
					(byte) 4, (byte) 5, (byte) 6, Byte.MAX_VALUE);

			addTests(suite, ColumnType.SHORT, Short[]::new, Short.MIN_VALUE, (short) 0, (short) 2, (short) 1, (short) 3,
					(short) 4, (short) 5, (short) 6, Short.MAX_VALUE);

			addTests(suite, ColumnType.INT, Integer[]::new, Integer.MIN_VALUE, 0, 2, 1, 3, 4, 5, 6, Integer.MAX_VALUE);

			addTests(suite, ColumnType.LONG, Long[]::new, Long.MIN_VALUE, 0L, 2L, 1L, 3L, 4L, 5L, 6L, Long.MAX_VALUE);

			addTests(suite, ColumnType.FLOAT, Float[]::new, Float.NEGATIVE_INFINITY, 0f, 2f, 1f, 3f, 4f, 5f, 6f,
					Float.POSITIVE_INFINITY);

			addTests(suite, ColumnType.DOUBLE, Double[]::new, Double.NEGATIVE_INFINITY, 0d, 2d, 1d, 3d, 4d, 5d, 6d,
					Double.POSITIVE_INFINITY);

			addTests(suite, ColumnType.DECIMAL, BigDecimal[]::new, new BigDecimal(-1), new BigDecimal(0),
					new BigDecimal(2), new BigDecimal(1), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5),
					new BigDecimal(6), new BigDecimal(7));

			LocalDate[] dates = new LocalDate[9];
			dates[0] = LocalDate.parse("2019-01-01");
			dates[1] = LocalDate.parse("2019-12-31");
			dates[2] = LocalDate.parse("2020-01-02");
			dates[3] = LocalDate.parse("2020-01-01");
			dates[4] = LocalDate.parse("2020-01-03");
			dates[5] = LocalDate.parse("2020-01-04");
			dates[6] = LocalDate.parse("2020-01-05");
			dates[7] = LocalDate.parse("2021-01-01");
			dates[8] = LocalDate.parse("2021-12-31");

			addTests(suite, ColumnType.DATE, LocalDate[]::new, dates);

			LocalDateTime[] dateTimes = Arrays.stream(dates).map(LocalDate::atStartOfDay).toArray(LocalDateTime[]::new);

			addTests(suite, ColumnType.DATETIME, LocalDateTime[]::new, dateTimes);

			addTests(suite, ColumnType.UUID, UUID[]::new, new UUID(0L, Long.MIN_VALUE), new UUID(0L, 0L),
					new UUID(0L, 2L), new UUID(0L, 1L), new UUID(0L, 3L), new UUID(0L, 4L), new UUID(0L, 5L),
					new UUID(0L, 6L), new UUID(0L, Long.MAX_VALUE));

			return suite;
		}

	}

	@SafeVarargs
	private static <E extends Comparable<? super E>> void addTests(TestSuite suite, ColumnType<E> type,
			IntFunction<E[]> newArray, E... s) {

		final E belowLesser = s[0];
		final E belowGreater = s[1];
		final SampleElements<E> samples = new SampleElements<E>(s[2], s[3], s[4], s[5], s[6]);
		final E aboveLesser = s[7];
		final E aboveGreater = s[8];

		suite.addTest(ListTestSuiteBuilder.using(new TestColumnAsListGenerator<>(type, samples, newArray, 0))
				.named(type.toString()).withFeatures(CollectionSize.ANY)
				.withFeatures(CollectionFeature.ALLOWS_NULL_VALUES, CollectionFeature.ALLOWS_NULL_QUERIES)
				.suppressing(LIST_TEST_HASH_CODE).createTestSuite());

		suite.addTest(ListTestSuiteBuilder.using(new TestColumnAsListGenerator<>(type, samples, newArray, NONNULL))
				.named(type + " - NONNULL").withFeatures(CollectionSize.ANY)
				.withFeatures(CollectionFeature.ALLOWS_NULL_QUERIES).suppressing(LIST_TEST_HASH_CODE)
				.createTestSuite());

		suite.addTest(
				ListTestSuiteBuilder.using(new TestColumnAsListGenerator<>(type, samples, newArray, NONNULL | SORTED))
						.named(type + " - SORTED").withFeatures(CollectionSize.ANY)
						.withFeatures(CollectionFeature.ALLOWS_NULL_QUERIES).suppressing(LIST_TEST_HASH_CODE)
						.createTestSuite());

		suite.addTest(ListTestSuiteBuilder
				.using(new TestColumnAsListGenerator<>(type, samples, newArray, NONNULL | SORTED | DISTINCT))
				.named(type + " - DISTINCT").withFeatures(CollectionSize.ANY)
				.withFeatures(CollectionFeature.ALLOWS_NULL_QUERIES, CollectionFeature.REJECTS_DUPLICATES_AT_CREATION)
				.suppressing(LIST_TEST_HASH_CODE, LIST_TEST_EQUALS_SET).createTestSuite());

		suite.addTest(NavigableSetTestSuiteBuilder.using(new TestSortedSetGenerator<E>() {
			@Override
			public SampleElements<E> samples() {
				return samples;
			}

			@Override
			public E[] createArray(int length) {
				return newArray.apply(length);
			}

			@Override
			public Iterable<E> order(List<E> insertionOrder) {
				Collections.sort(insertionOrder);
				return insertionOrder;
			}

			@Override
			public NavigableSet<E> create(Object... elements) {

				ColumnBuilder<E> builder = type.builder(NONNULL);

				for (Object o : elements) {
					@SuppressWarnings("unchecked")
					E e = (E) o;
					builder.add(e);
				}

				return builder.build().toDistinct().asSet();
			}

			@Override
			public E belowSamplesLesser() {
				return belowLesser;
			}

			@Override
			public E belowSamplesGreater() {
				return belowGreater;
			}

			@Override
			public E aboveSamplesLesser() {
				return aboveLesser;
			}

			@Override
			public E aboveSamplesGreater() {
				return aboveGreater;
			}

		}).named(type + " - AS_SET").withFeatures(CollectionSize.ANY)
				.withFeatures(CollectionFeature.ALLOWS_NULL_QUERIES).createTestSuite());
	}

	private static class TestColumnAsListGenerator<E extends Comparable<? super E>> implements TestListGenerator<E> {

		final ColumnType<E> type;
		final SampleElements<E> samples;
		final IntFunction<E[]> newArray;
		final int characteristics;

		private TestColumnAsListGenerator(ColumnType<E> type, SampleElements<E> samples, IntFunction<E[]> newArray,
				int characteristics) {
			this.type = type;
			this.samples = samples;
			this.newArray = newArray;
			this.characteristics = characteristics;
		}

		@Override
		public SampleElements<E> samples() {
			return samples;
		}

		@Override
		public E[] createArray(int length) {
			return newArray.apply(length);
		}

		@Override
		public Iterable<E> order(List<E> insertionOrder) {
			if ((characteristics & SORTED) != 0)
				Collections.sort(insertionOrder);
			return insertionOrder;
		}

		@Override
		public List<E> create(Object... elements) {

			ColumnBuilder<E> builder = type.builder((characteristics & NONNULL) != 0 ? NONNULL : 0);

			for (Object o : elements) {
				@SuppressWarnings("unchecked")
				E e = (E) o;
				builder.add(e);
			}

			Column<E> column = builder.build();
			if ((characteristics & DISTINCT) != 0)
				return column.toDistinct();
			else if ((characteristics & SORTED) != 0)
				return column.toSorted();
			else
				return column;
		}
	}

	private static Method suppressMethod(Class<?> testClass, String methodName) {
		try {
			return testClass.getMethod(methodName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}