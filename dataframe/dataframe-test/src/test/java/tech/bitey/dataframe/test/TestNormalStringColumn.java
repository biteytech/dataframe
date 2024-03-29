/*
 * Copyright 2022 biteytech@protonmail.com
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

package tech.bitey.dataframe.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.BooleanColumn;
import tech.bitey.dataframe.ByteColumn;
import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.DateColumn;
import tech.bitey.dataframe.DateTimeColumn;
import tech.bitey.dataframe.DecimalColumn;
import tech.bitey.dataframe.DoubleColumn;
import tech.bitey.dataframe.FloatColumn;
import tech.bitey.dataframe.IntColumn;
import tech.bitey.dataframe.LongColumn;
import tech.bitey.dataframe.NormalStringColumn;
import tech.bitey.dataframe.ShortColumn;
import tech.bitey.dataframe.StringColumn;

public class TestNormalStringColumn extends TestColumn<String> {

	private static final Random RAND = new Random(0);

	TestNormalStringColumn() {
		super("String.MIN_VALUE", "String.MAX_VALUE", String[]::new);
	}

	@Override
	Column<String> parseColumn(StringColumn stringColumn) {
		return stringColumn.toNormalStringColumn();
	}

	@Override
	TestSample<String> wrapSample(String label, String[] array, int characteristics) {
		NormalStringColumn column = NormalStringColumn.builder().addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}

	@Override
	TestSample<String> wrapSample(String label, String[] array, Column<String> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override
	public void testToSorted() {
	}

	@Override
	public void testToDistinct() {
	}

	@Override
	String[] toArray(Collection<String> samples) {
		return samples.toArray(empty());
	}

	@Override
	Column<String> collect(Stream<String> stream) {
		return stream.collect(NormalStringColumn.collector());
	}

	@Override
	String[] random(int size) {
		List<String> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new String[0]);
	}

	@Override
	String[] NXNX(int size) {
		String[] random = random(size);
		for (int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	String[] NNXX(int size) {
		String[] random = random(size);
		for (int i = 0; i < size; i += 4) {
			random[i] = null;
			if (i + 1 < size)
				random[i + 1] = null;
		}
		return random;
	}

	@Override
	String[] sequential(int size) {
		String[] elements = new String[size];
		for (int i = 0; i < size; i++)
			elements[i] = String.valueOf(i);
		Arrays.sort(elements);
		return elements;
	}

	@Override
	String[] same(int size) {
		String[] elements = new String[size];
		for (int i = 0; i < size; i++)
			elements[i] = String.valueOf(0);
		return elements;
	}

	@Override
	String[] smar(int size) {
		String[] elements = new String[size];
		for (int i = 1, n = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = String.valueOf(n);
		return elements;
	}

	@Override
	String[] notPresent() {
		return new String[] { "-2378457", "-347700", "-88" };
	}

	@Test
	public void testParse() {

		NormalStringColumn s = NormalStringColumn.of(null, "true", null, "false", null).subColumn(1, 4);
		assertEquals(BooleanColumn.of(true, null, false), s.parseBoolean(), "parseBoolean");

		s = NormalStringColumn.of("2020-05-04");
		assertEquals(DateColumn.of(LocalDate.parse("2020-05-04")), s.parseDate(), "parseDate");

		s = NormalStringColumn.of("2020-05-04T10:01");
		assertEquals(DateTimeColumn.of(LocalDateTime.parse("2020-05-04T10:01")), s.parseDateTime(), "parseDateTime");

		s = NormalStringColumn.of("1.0", "NaN", "-5e5");
		assertEquals(DoubleColumn.of(1.0, Double.NaN, -500000.0), s.parseDouble(), "parseDouble");

		s = NormalStringColumn.of("1.0", "NaN", "-5e5");
		assertEquals(FloatColumn.of(1f, Float.NaN, -500000f), s.parseFloat(), "parseFloat");

		s = NormalStringColumn.of("-1", "0", "" + Integer.MAX_VALUE);
		assertEquals(IntColumn.of(-1, 0, Integer.MAX_VALUE), s.parseInt(), "parseInt");

		s = NormalStringColumn.of("-1", "0", "" + Long.MAX_VALUE);
		assertEquals(LongColumn.of(-1L, 0L, Long.MAX_VALUE), s.parseLong(), "parseLong");

		s = NormalStringColumn.of("-1", "0", "" + Short.MAX_VALUE);
		assertEquals(ShortColumn.of((short) -1, (short) 0, Short.MAX_VALUE), s.parseShort(), "parseShort");

		s = NormalStringColumn.of("-1", "0", "" + Byte.MAX_VALUE);
		assertEquals(ByteColumn.of((byte) -1, (byte) 0, Byte.MAX_VALUE), s.parseByte(), "parseByte");

		s = NormalStringColumn.of("-1", "0");
		assertEquals(DecimalColumn.of(BigDecimal.ONE.negate(), BigDecimal.ZERO), s.parseDecimal(), "parseDecmial");
	}

	// 1026 values
	private static final String[] RANDOM = { "5", "5", "8", "5", "2", "6", "9", "2", "2", "8", "7", "1", "5", "1", "3",
			"4", "8", "8", "3", "1", "5", "6", "1", "2", "1", "0", "1", "4", "5", "9", "1", "2", "2", "9", "7", "8",
			"8", "6", "2", "0", "3", "9", "6", "2", "6", "5", "9", "8", "8", "6", "3", "8", "9", "3", "8", "7", "2",
			"1", "5", "9", "2", "2", "5", "0", "6", "1", "8", "8", "6", "0", "1", "8", "6", "2", "2", "4", "1", "0",
			"7", "4", "3", "1", "3", "3", "4", "0", "7", "4", "7", "9", "6", "9", "5", "6", "6", "8", "1", "2", "7",
			"2", "3", "0", "4", "2", "5", "6", "2", "7", "1", "5", "3", "3", "4", "8", "8", "3", "2", "5", "6", "7",
			"6", "0", "3", "7", "0", "3", "9", "8", "6", "1", "5", "3", "6", "4", "3", "4", "3", "2", "9", "6", "3",
			"7", "6", "3", "1", "8", "7", "2", "0", "1", "6", "2", "4", "5", "7", "4", "1", "2", "4", "8", "7", "2",
			"3", "7", "4", "1", "1", "1", "8", "9", "5", "1", "2", "2", "3", "0", "1", "8", "9", "9", "0", "2", "1",
			"2", "3", "2", "0", "4", "5", "2", "9", "6", "7", "6", "1", "3", "0", "9", "5", "5", "41", "41", "14", "48",
			"23", "48", "2", "93", "46", "62", "70", "82", "27", "80", "31", "85", "64", "51", "48", "37", "68", "17",
			"49", "95", "0", "2", "37", "88", "61", "88", "59", "90", "75", "23", "55", "0", "15", "36", "91", "96",
			"84", "53", "81", "32", "12", "15", "27", "57", "85", "8", "2", "13", "44", "60", "53", "40", "70", "75",
			"70", "89", "6", "47", "35", "90", "72", "53", "3", "42", "0", "87", "0", "35", "39", "49", "40", "54",
			"70", "44", "56", "47", "72", "75", "97", "93", "62", "51", "19", "26", "71", "98", "1", "36", "23", "60",
			"94", "14", "21", "97", "14", "94", "84", "27", "40", "87", "87", "28", "12", "62", "22", "37", "42", "37",
			"73", "23", "74", "77", "27", "29", "83", "19", "60", "3", "10", "27", "11", "46", "88", "24", "7", "62",
			"96", "51", "74", "61", "96", "6", "60", "60", "96", "73", "59", "33", "61", "29", "16", "96", "75", "73",
			"18", "79", "1", "94", "15", "67", "22", "82", "14", "60", "69", "62", "12", "61", "82", "34", "72", "68",
			"72", "18", "7", "37", "16", "2", "50", "71", "68", "26", "45", "95", "82", "92", "24", "66", "92", "92",
			"57", "41", "60", "62", "61", "92", "0", "39", "98", "12", "99", "38", "31", "6", "8", "18", "954", "598",
			"453", "982", "918", "345", "529", "8", "928", "591", "726", "23", "368", "744", "873", "307", "890", "650",
			"716", "175", "260", "772", "362", "487", "52", "794", "172", "516", "918", "222", "700", "830", "194",
			"663", "654", "573", "764", "209", "30", "395", "869", "129", "972", "177", "407", "938", "692", "637",
			"806", "497", "658", "157", "378", "625", "323", "859", "161", "620", "463", "668", "617", "807", "651",
			"174", "219", "265", "995", "366", "955", "344", "348", "652", "300", "792", "382", "771", "913", "888",
			"665", "809", "829", "282", "311", "295", "284", "383", "615", "821", "131", "181", "530", "6", "610",
			"191", "310", "83", "362", "215", "376", "950", "680", "106", "798", "533", "873", "808", "187", "945",
			"593", "174", "59", "67", "292", "474", "614", "2", "877", "291", "810", "979", "452", "58", "848", "341",
			"538", "656", "649", "903", "201", "326", "770", "369", "615", "763", "194", "20", "617", "334", "940",
			"503", "3", "400", "943", "455", "45", "748", "115", "697", "834", "366", "215", "407", "722", "486", "390",
			"222", "943", "640", "79", "272", "973", "689", "435", "618", "939", "967", "378", "132", "374", "965",
			"686", "328", "324", "450", "505", "41", "501", "45", "483", "609", "278", "359", "307", "39", "728", "749",
			"102", "412", "485", "513", "102", "480", "34", "436", "466", "566", "824", "874", "771", "513", "3829",
			"4769", "7270", "7857", "7840", "1671", "4653", "2781", "7553", "3904", "4201", "1686", "3966", "2313",
			"8842", "2798", "1850", "3652", "5214", "4468", "9895", "3152", "8496", "7581", "8893", "1508", "5272",
			"7009", "5587", "8440", "2449", "511", "1370", "4719", "5031", "8093", "7946", "6517", "143", "7904",
			"2003", "5058", "4993", "8612", "917", "2280", "6304", "7944", "7563", "8318", "3834", "3402", "1906",
			"3337", "7306", "7755", "7944", "5716", "4107", "6814", "4032", "1772", "2119", "546", "8354", "9179",
			"5470", "8793", "3834", "3402", "1906", "3337", "7306", "7755", "7944", "5716", "4107", "6814", "3834",
			"3402", "1906", "3337", "7306", "7755", "7944", "5716", "4107", "6814", "974", "4815", "6578", "6356",
			"5713", "9541", "3375", "8398", "6096", "1831", "1976", "9762", "6849", "1760", "7372", "2793", "6314",
			"2824", "5144", "9988", "1905", "3761", "1750", "2242", "8386", "7548", "6339", "9326", "2687", "3384",
			"6830", "5897", "1489", "2552", "3531", "831", "6237", "8192", "9992", "3321", "2482", "4124", "5351",
			"4510", "5535", "6957", "1914", "9638", "610", "5713", "8157", "4317", "170", "7373", "6893", "6593",
			"3614", "3834", "9383", "9832", "4278", "2202", "8059", "7664", "7639", "3237", "8350", "964", "3661",
			"1236", "1855", "8488", "1600", "1465", "5569", "962", "1417", "6057", "2055", "4997", "4510", "248",
			"5855", "3323", "7225", "751", "917", "7454", "5319", "5500", "8840", "3427", "6396", "7718", "3337",
			"3283", "8979", "5418", "3327", "9239", "1571", "7271", "6408", "8752", "5198", "4676", "8138", "5665",
			"9907", "9849", "2744", "6137", "2755", "8928", "8000", "1425", "2048", "185", "5361", "3022", "4193",
			"3860", "9785", "867", "3891", "7743", "6684", "550", "6874", "9836", "1554", "2589", "39285", "67053",
			"64215", "25493", "49807", "95711", "81334", "88735", "54753", "80832", "89535", "79607", "76932", "26335",
			"55013", "94307", "30501", "17279", "75973", "98388", "95074", "51447", "30752", "78140", "72689", "39913",
			"78729", "95948", "59551", "53437", "19898", "50696", "8213", "98009", "53473", "75914", "91031", "1727",
			"34232", "41616", "97287", "64675", "58962", "40323", "72742", "94855", "63441", "78789", "38298", "54997",
			"44213", "2550", "61227", "55353", "8335", "90850", "97896", "66713", "66259", "61098", "80679", "35857",
			"30796", "17422", "45833", "72751", "99369", "25044", "72603", "6056", "19829", "25240", "94265", "74325",
			"50149", "63473", "33581", "9070", "15882", "71195", "5205", "56609", "77094", "94309", "20675", "95822",
			"22523", "31545", "62366", "57026", "20287", "89028", "49765", "20878", "6444", "23266", "34740", "196",
			"5396", "52007", "62463", "51566", "41681", "1228", "19899", "33450", "45594", "81492", "2268", "82502",
			"40434", "49561", "56985", "42740", "84603", "62680", "46796", "48010", "50920", "49238", "30229", "85913",
			"74192", "54797", "31614", "88455", "11933", "52024", "85657", "85963", "98564", "75351", "15366", "29989",
			"75632", "68766", "62705", "38071", "73079", "93856", "62744", "93156", "23790", "20820", "11401", "516",
			"52632", "29825", "91786", "72513", "31673", "12940", "6291", "95377", "79434", "15130", "85717", "15504",
			"3493", "46596", "54590", "39161", "63871", "4535", "97108", "19040", "85877", "71012", "38303", "26448",
			"53343", "59243", "69092", "82236", "15618", "52766", "33739", "68100", "18082", "61077", "3714", "3654",
			"74484", "17134", "63098", "14302", "22289", "76991", "11183", "81724", "14726", "10746", "23110", "32097",
			"40776", "54939", "60687", "17397", "95302", "68113", "String.MAX_VALUE", "String.MAX_VALUE",
			"String.MAX_VALUE", "String.MIN_VALUE", "String.MIN_VALUE", "String.MIN_VALUE" };
}
