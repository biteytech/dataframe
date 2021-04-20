/*
 * Copyright 2021 biteytech@protonmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.bitey.dataframe.guava;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.IntFunction;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.Maps;
import com.google.common.collect.testing.Helpers;
import com.google.common.collect.testing.ListTestSuiteBuilder;
import com.google.common.collect.testing.NavigableMapTestSuiteBuilder;
import com.google.common.collect.testing.NavigableSetTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestListGenerator;
import com.google.common.collect.testing.TestSortedMapGenerator;
import com.google.common.collect.testing.TestSortedSetGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.MapFeature;
import com.google.common.collect.testing.testers.ListEqualsTester;
import com.google.common.collect.testing.testers.ListHashCodeTester;
import com.google.common.collect.testing.testers.MapHashCodeTester;
import com.google.common.collect.testing.testers.SetHashCodeTester;

import junit.framework.TestSuite;
import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.ColumnBuilder;
import tech.bitey.dataframe.ColumnType;
import tech.bitey.dataframe.DataFrame;
import tech.bitey.dataframe.DataFrameFactory;

@RunWith(Suite.class)
@Suite.SuiteClasses({ GuavaTestLibSuite.ColumnTests.class, GuavaTestLibSuite.ColumnMapTests.class })
public class GuavaTestLibSuite {

	private static final Method LIST_TEST_HASH_CODE = suppressMethod(ListHashCodeTester.class, "testHashCode");
	private static final Method LIST_TEST_EQUALS_SET = suppressMethod(ListEqualsTester.class, "testEquals_set");
	private static final Method SET_TEST_HASH_CODE = suppressMethod(SetHashCodeTester.class, "testHashCode");
	private static final Method MAP_TEST_HASH_CODE = suppressMethod(MapHashCodeTester.class, "testHashCode");
	private static final Method MAP_TEST_HASH_CODE_NULL = suppressMethod(MapHashCodeTester.class,
			"testHashCode_containingNullValue");

	private static class TestIngredients<E extends Comparable<? super E>> {
		final ColumnType<E> type;
		final IntFunction<E[]> newArray;
		final E[] s;

		@SafeVarargs
		TestIngredients(ColumnType<E> type, IntFunction<E[]> newArray, E... s) {
			this.type = type;
			this.newArray = newArray;
			this.s = s;
		}
	}

	private static LocalDate[] DATES = new LocalDate[] { LocalDate.parse("2019-01-01"), LocalDate.parse("2019-12-31"),
			LocalDate.parse("2020-01-02"), LocalDate.parse("2020-01-01"), LocalDate.parse("2020-01-03"),
			LocalDate.parse("2020-01-04"), LocalDate.parse("2020-01-05"), LocalDate.parse("2021-01-01"),
			LocalDate.parse("2021-12-31") };

	private static final TestIngredients<?>[] INGREDIENTS = new TestIngredients<?>[] {
			new TestIngredients<>(ColumnType.STRING, String[]::new, "!! a", "!! b", "b", "a", "c", "d", "e", "~~ a",
					"~~ b"),
			new TestIngredients<>(ColumnType.BYTE, Byte[]::new, Byte.MIN_VALUE, (byte) 0, (byte) 2, (byte) 1, (byte) 3,
					(byte) 4, (byte) 5, (byte) 6, Byte.MAX_VALUE),
			new TestIngredients<>(ColumnType.SHORT, Short[]::new, Short.MIN_VALUE, (short) 0, (short) 2, (short) 1,
					(short) 3, (short) 4, (short) 5, (short) 6, Short.MAX_VALUE),
			new TestIngredients<>(ColumnType.INT, Integer[]::new, Integer.MIN_VALUE, 0, 2, 1, 3, 4, 5, 6,
					Integer.MAX_VALUE),
			new TestIngredients<>(ColumnType.LONG, Long[]::new, Long.MIN_VALUE, 0L, 2L, 1L, 3L, 4L, 5L, 6L,
					Long.MAX_VALUE),
			new TestIngredients<>(ColumnType.FLOAT, Float[]::new, Float.NEGATIVE_INFINITY, 0f, 2f, 1f, 3f, 4f, 5f, 6f,
					Float.POSITIVE_INFINITY),
			new TestIngredients<>(ColumnType.DOUBLE, Double[]::new, Double.NEGATIVE_INFINITY, 0d, 2d, 1d, 3d, 4d, 5d,
					6d, Double.POSITIVE_INFINITY),
			new TestIngredients<>(ColumnType.DECIMAL, BigDecimal[]::new, new BigDecimal(-1), new BigDecimal(0),
					new BigDecimal(2), new BigDecimal(1), new BigDecimal(3), new BigDecimal(4), new BigDecimal(5),
					new BigDecimal(6), new BigDecimal(7)),
			new TestIngredients<>(ColumnType.DATE, LocalDate[]::new, DATES),
			new TestIngredients<>(ColumnType.DATETIME, LocalDateTime[]::new,
					Arrays.stream(DATES).map(LocalDate::atStartOfDay).toArray(LocalDateTime[]::new)),
			new TestIngredients<>(ColumnType.TIME, LocalTime[]::new, LocalTime.MIN, LocalTime.MIN.plusNanos(1),
					LocalTime.of(1, 0), LocalTime.of(1, 1), LocalTime.of(8, 8), LocalTime.NOON.minusNanos(1),
					LocalTime.NOON.plusNanos(1), LocalTime.MAX.minusNanos(1), LocalTime.MAX),
			new TestIngredients<>(ColumnType.UUID, UUID[]::new, new UUID(0L, Long.MIN_VALUE), new UUID(0L, 0L),
					new UUID(0L, 2L), new UUID(0L, 1L), new UUID(0L, 3L), new UUID(0L, 4L), new UUID(0L, 5L),
					new UUID(0L, 6L), new UUID(0L, Long.MAX_VALUE)) };

	public static class ColumnTests {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public static TestSuite suite() {

			TestSuite suite = new TestSuite("ColumnTests");

			for (TestIngredients i : INGREDIENTS)
				addColumnTests(suite, i);

			return suite;
		}
	}

	private static <E extends Comparable<? super E>> void addColumnTests(TestSuite suite,
			TestIngredients<E> ingredients) {

		final E belowLesser = ingredients.s[0];
		final E belowGreater = ingredients.s[1];
		final SampleElements<E> samples = new SampleElements<E>(ingredients.s[2], ingredients.s[3], ingredients.s[4],
				ingredients.s[5], ingredients.s[6]);
		final E aboveLesser = ingredients.s[7];
		final E aboveGreater = ingredients.s[8];

		final ColumnType<E> type = ingredients.type;
		final IntFunction<E[]> newArray = ingredients.newArray;

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

	public static class ColumnMapTests {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public static TestSuite suite() {

			TestSuite suite = new TestSuite("ColumnMapTests");

			for (TestIngredients i : INGREDIENTS)
				addColumnMapTests(suite, i);

			return suite;
		}
	}

	private static <E extends Comparable<? super E>> void addColumnMapTests(TestSuite suite,
			TestIngredients<E> ingredients) {

		suite.addTest(NavigableMapTestSuiteBuilder.using(new TestColumnMapGenerator<>(ingredients))
				.named(ingredients.type + " - COLMAP")
				.withFeatures(MapFeature.ALLOWS_NULL_VALUE_QUERIES, MapFeature.ALLOWS_NULL_VALUES, CollectionSize.ANY)
				.suppressing(MAP_TEST_HASH_CODE, MAP_TEST_HASH_CODE_NULL, SET_TEST_HASH_CODE).createTestSuite());
	}

	private static class TestColumnMapGenerator<E extends Comparable<? super E>>
			implements TestSortedMapGenerator<E, E> {

		final ColumnType<E> type;
		final SampleElements<Entry<E, E>> samples;
		final IntFunction<E[]> newArray;
		final Entry<E, E> belowLesser;
		final Entry<E, E> belowGreater;
		final Entry<E, E> aboveLesser;
		final Entry<E, E> aboveGreater;

		private TestColumnMapGenerator(TestIngredients<E> ingredients) {
			this.type = ingredients.type;
			this.newArray = ingredients.newArray;

			final E[] s = ingredients.s;
			belowLesser = Helpers.mapEntry(s[0], s[0]);
			belowGreater = Helpers.mapEntry(s[1], s[1]);
			samples = new SampleElements<Entry<E, E>>(Helpers.mapEntry(s[2], s[2]), Helpers.mapEntry(s[3], s[3]),
					Helpers.mapEntry(s[4], s[4]), Helpers.mapEntry(s[5], s[5]), Helpers.mapEntry(s[6], s[6]));
			aboveLesser = Helpers.mapEntry(s[7], s[7]);
			aboveGreater = Helpers.mapEntry(s[8], s[8]);
		}

		@Override
		public E[] createKeyArray(int length) {
			return newArray.apply(length);
		}

		@Override
		public E[] createValueArray(int length) {
			return newArray.apply(length);
		}

		@Override
		public SampleElements<Entry<E, E>> samples() {
			return samples;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Entry<E, E>[] createArray(int length) {
			return new Entry[length];
		}

		@Override
		public Iterable<Entry<E, E>> order(List<Entry<E, E>> insertionOrder) {
			Collections.sort(insertionOrder, (e1, e2) -> e1.getKey().compareTo(e2.getKey()));
			return insertionOrder;
		}

		@Override
		public SortedMap<E, E> create(Object... elements) {

			Map<E, E> map = Maps.newHashMap();

			for (Object o : elements) {
				@SuppressWarnings("unchecked")
				Entry<E, E> e = (Entry<E, E>) o;
				map.put(e.getKey(), e.getValue());
			}

			ColumnBuilder<E> keyBuilder = type.builder(NONNULL);
			ColumnBuilder<E> valBuilder = type.builder();

			for (Map.Entry<E, E> e : map.entrySet()) {
				keyBuilder.add(e.getKey());
				valBuilder.add(e.getValue());
			}

			DataFrame df = DataFrameFactory.create(new Column<?>[] { keyBuilder.build(), valBuilder.build() },
					new String[] { "KEYS", "VALUES" }).indexOrganize(0);
			return df.asMap(1);
		}

		@Override
		public Entry<E, E> belowSamplesLesser() {
			return belowLesser;
		}

		@Override
		public Entry<E, E> belowSamplesGreater() {
			return belowGreater;
		}

		@Override
		public Entry<E, E> aboveSamplesLesser() {
			return aboveLesser;
		}

		@Override
		public Entry<E, E> aboveSamplesGreater() {
			return aboveGreater;
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