package tech.bitey.dataframe;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TestBigDecimalColumn extends TestColumn<BigDecimal> {

	private static final Random RAND = new Random(0);

	private static final BigDecimal MIN_VALUE = BigDecimal.valueOf(Integer.MIN_VALUE);
	private static final BigDecimal MAX_VALUE = BigDecimal.valueOf(Integer.MAX_VALUE);

	@Override
	TestSample<BigDecimal> wrapSample(String label, BigDecimal[] array, int characteristics) {
		DecimalColumn column = DecimalColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}

	@Override
	TestSample<BigDecimal> wrapSample(String label, BigDecimal[] array, Column<BigDecimal> column, int fromIndex,
			int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override
	BigDecimal[] toArray(Collection<BigDecimal> samples) {
		return samples.toArray(empty());
	}

	@Override
	BigDecimal[] empty() {
		return new BigDecimal[0];
	}

	@Override
	BigDecimal[] singleNull() {
		return new BigDecimal[] { null };
	}

	@Override
	BigDecimal[] singleNonNull() {
		return new BigDecimal[] { ZERO };
	}

	@Override
	BigDecimal[] duoFirstNull() {
		return new BigDecimal[] { null, ZERO };
	}

	@Override
	BigDecimal[] duoBothNull() {
		return new BigDecimal[] { null, null };
	}

	@Override
	BigDecimal[] duoDistinct() {
		return new BigDecimal[] { ZERO, ONE };
	}

	@Override
	BigDecimal[] duoSame() {
		return new BigDecimal[] { ZERO, ZERO };
	}

	@Override
	BigDecimal[] minMax() {
		return new BigDecimal[] { MIN_VALUE, MAX_VALUE };
	}

	@Override
	BigDecimal[] allNull(int size) {
		return new BigDecimal[size];
	}

	@Override
	BigDecimal[] random(int size) {
		List<BigDecimal> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new BigDecimal[0]);
	}

	@Override
	BigDecimal[] NXNX(int size) {
		BigDecimal[] random = random(size);
		for (int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	BigDecimal[] NNXX(int size) {
		BigDecimal[] random = random(size);
		for (int i = 0; i < size; i += 4) {
			random[i] = null;
			if (i + 1 < size)
				random[i + 1] = null;
		}
		return random;
	}

	@Override
	BigDecimal[] sequential(int size) {
		BigDecimal[] elements = new BigDecimal[size];
		for (int i = 0; i < size; i++)
			elements[i] = BigDecimal.valueOf(i);
		Arrays.sort(elements);
		return elements;
	}

	@Override
	BigDecimal[] same(int size) {
		BigDecimal[] elements = new BigDecimal[size];
		for (int i = 0; i < size; i++)
			elements[i] = ZERO;
		return elements;
	}

	@Override
	BigDecimal[] smar(int size) {
		BigDecimal[] elements = new BigDecimal[size];
		for (int i = 1, n = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = BigDecimal.valueOf(n);
		return elements;
	}

	@Override
	BigDecimal[] notPresent() {
		return new BigDecimal[] { new BigDecimal("-2378457"), new BigDecimal("-347700"), new BigDecimal("-88") };
	}

	// 1026 values
	private static final BigDecimal[] RANDOM = { new BigDecimal("5"), new BigDecimal("5"), new BigDecimal("8"),
			new BigDecimal("5"), new BigDecimal("2"), new BigDecimal("6"), new BigDecimal("9"), new BigDecimal("2"),
			new BigDecimal("2"), new BigDecimal("8"), new BigDecimal("7"), new BigDecimal("1"), new BigDecimal("5"),
			new BigDecimal("1"), new BigDecimal("3"), new BigDecimal("4"), new BigDecimal("8"), new BigDecimal("8"),
			new BigDecimal("3"), new BigDecimal("1"), new BigDecimal("5"), new BigDecimal("6"), new BigDecimal("1"),
			new BigDecimal("2"), new BigDecimal("1"), new BigDecimal("0"), new BigDecimal("1"), new BigDecimal("4"),
			new BigDecimal("5"), new BigDecimal("9"), new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("2"),
			new BigDecimal("9"), new BigDecimal("7"), new BigDecimal("8"), new BigDecimal("8"), new BigDecimal("6"),
			new BigDecimal("2"), new BigDecimal("0"), new BigDecimal("3"), new BigDecimal("9"), new BigDecimal("6"),
			new BigDecimal("2"), new BigDecimal("6"), new BigDecimal("5"), new BigDecimal("9"), new BigDecimal("8"),
			new BigDecimal("8"), new BigDecimal("6"), new BigDecimal("3"), new BigDecimal("8"), new BigDecimal("9"),
			new BigDecimal("3"), new BigDecimal("8"), new BigDecimal("7"), new BigDecimal("2"), new BigDecimal("1"),
			new BigDecimal("5"), new BigDecimal("9"), new BigDecimal("2"), new BigDecimal("2"), new BigDecimal("5"),
			new BigDecimal("0"), new BigDecimal("6"), new BigDecimal("1"), new BigDecimal("8"), new BigDecimal("8"),
			new BigDecimal("6"), new BigDecimal("0"), new BigDecimal("1"), new BigDecimal("8"), new BigDecimal("6"),
			new BigDecimal("2"), new BigDecimal("2"), new BigDecimal("4"), new BigDecimal("1"), new BigDecimal("0"),
			new BigDecimal("7"), new BigDecimal("4"), new BigDecimal("3"), new BigDecimal("1"), new BigDecimal("3"),
			new BigDecimal("3"), new BigDecimal("4"), new BigDecimal("0"), new BigDecimal("7"), new BigDecimal("4"),
			new BigDecimal("7"), new BigDecimal("9"), new BigDecimal("6"), new BigDecimal("9"), new BigDecimal("5"),
			new BigDecimal("6"), new BigDecimal("6"), new BigDecimal("8"), new BigDecimal("1"), new BigDecimal("2"),
			new BigDecimal("7"), new BigDecimal("2"), new BigDecimal("3"), new BigDecimal("0"), new BigDecimal("4"),
			new BigDecimal("2"), new BigDecimal("5"), new BigDecimal("6"), new BigDecimal("2"), new BigDecimal("7"),
			new BigDecimal("1"), new BigDecimal("5"), new BigDecimal("3"), new BigDecimal("3"), new BigDecimal("4"),
			new BigDecimal("8"), new BigDecimal("8"), new BigDecimal("3"), new BigDecimal("2"), new BigDecimal("5"),
			new BigDecimal("6"), new BigDecimal("7"), new BigDecimal("6"), new BigDecimal("0"), new BigDecimal("3"),
			new BigDecimal("7"), new BigDecimal("0"), new BigDecimal("3"), new BigDecimal("9"), new BigDecimal("8"),
			new BigDecimal("6"), new BigDecimal("1"), new BigDecimal("5"), new BigDecimal("3"), new BigDecimal("6"),
			new BigDecimal("4"), new BigDecimal("3"), new BigDecimal("4"), new BigDecimal("3"), new BigDecimal("2"),
			new BigDecimal("9"), new BigDecimal("6"), new BigDecimal("3"), new BigDecimal("7"), new BigDecimal("6"),
			new BigDecimal("3"), new BigDecimal("1"), new BigDecimal("8"), new BigDecimal("7"), new BigDecimal("2"),
			new BigDecimal("0"), new BigDecimal("1"), new BigDecimal("6"), new BigDecimal("2"), new BigDecimal("4"),
			new BigDecimal("5"), new BigDecimal("7"), new BigDecimal("4"), new BigDecimal("1"), new BigDecimal("2"),
			new BigDecimal("4"), new BigDecimal("8"), new BigDecimal("7"), new BigDecimal("2"), new BigDecimal("3"),
			new BigDecimal("7"), new BigDecimal("4"), new BigDecimal("1"), new BigDecimal("1"), new BigDecimal("1"),
			new BigDecimal("8"), new BigDecimal("9"), new BigDecimal("5"), new BigDecimal("1"), new BigDecimal("2"),
			new BigDecimal("2"), new BigDecimal("3"), new BigDecimal("0"), new BigDecimal("1"), new BigDecimal("8"),
			new BigDecimal("9"), new BigDecimal("9"), new BigDecimal("0"), new BigDecimal("2"), new BigDecimal("1"),
			new BigDecimal("2"), new BigDecimal("3"), new BigDecimal("2"), new BigDecimal("0"), new BigDecimal("4"),
			new BigDecimal("5"), new BigDecimal("2"), new BigDecimal("9"), new BigDecimal("6"), new BigDecimal("7"),
			new BigDecimal("6"), new BigDecimal("1"), new BigDecimal("3"), new BigDecimal("0"), new BigDecimal("9"),
			new BigDecimal("5"), new BigDecimal("5"), new BigDecimal("41"), new BigDecimal("41"), new BigDecimal("14"),
			new BigDecimal("48"), new BigDecimal("23"), new BigDecimal("48"), new BigDecimal("2"), new BigDecimal("93"),
			new BigDecimal("46"), new BigDecimal("62"), new BigDecimal("70"), new BigDecimal("82"),
			new BigDecimal("27"), new BigDecimal("80"), new BigDecimal("31"), new BigDecimal("85"),
			new BigDecimal("64"), new BigDecimal("51"), new BigDecimal("48"), new BigDecimal("37"),
			new BigDecimal("68"), new BigDecimal("17"), new BigDecimal("49"), new BigDecimal("95"), new BigDecimal("0"),
			new BigDecimal("2"), new BigDecimal("37"), new BigDecimal("88"), new BigDecimal("61"), new BigDecimal("88"),
			new BigDecimal("59"), new BigDecimal("90"), new BigDecimal("75"), new BigDecimal("23"),
			new BigDecimal("55"), new BigDecimal("0"), new BigDecimal("15"), new BigDecimal("36"), new BigDecimal("91"),
			new BigDecimal("96"), new BigDecimal("84"), new BigDecimal("53"), new BigDecimal("81"),
			new BigDecimal("32"), new BigDecimal("12"), new BigDecimal("15"), new BigDecimal("27"),
			new BigDecimal("57"), new BigDecimal("85"), new BigDecimal("8"), new BigDecimal("2"), new BigDecimal("13"),
			new BigDecimal("44"), new BigDecimal("60"), new BigDecimal("53"), new BigDecimal("40"),
			new BigDecimal("70"), new BigDecimal("75"), new BigDecimal("70"), new BigDecimal("89"), new BigDecimal("6"),
			new BigDecimal("47"), new BigDecimal("35"), new BigDecimal("90"), new BigDecimal("72"),
			new BigDecimal("53"), new BigDecimal("3"), new BigDecimal("42"), new BigDecimal("0"), new BigDecimal("87"),
			new BigDecimal("0"), new BigDecimal("35"), new BigDecimal("39"), new BigDecimal("49"), new BigDecimal("40"),
			new BigDecimal("54"), new BigDecimal("70"), new BigDecimal("44"), new BigDecimal("56"),
			new BigDecimal("47"), new BigDecimal("72"), new BigDecimal("75"), new BigDecimal("97"),
			new BigDecimal("93"), new BigDecimal("62"), new BigDecimal("51"), new BigDecimal("19"),
			new BigDecimal("26"), new BigDecimal("71"), new BigDecimal("98"), new BigDecimal("1"), new BigDecimal("36"),
			new BigDecimal("23"), new BigDecimal("60"), new BigDecimal("94"), new BigDecimal("14"),
			new BigDecimal("21"), new BigDecimal("97"), new BigDecimal("14"), new BigDecimal("94"),
			new BigDecimal("84"), new BigDecimal("27"), new BigDecimal("40"), new BigDecimal("87"),
			new BigDecimal("87"), new BigDecimal("28"), new BigDecimal("12"), new BigDecimal("62"),
			new BigDecimal("22"), new BigDecimal("37"), new BigDecimal("42"), new BigDecimal("37"),
			new BigDecimal("73"), new BigDecimal("23"), new BigDecimal("74"), new BigDecimal("77"),
			new BigDecimal("27"), new BigDecimal("29"), new BigDecimal("83"), new BigDecimal("19"),
			new BigDecimal("60"), new BigDecimal("3"), new BigDecimal("10"), new BigDecimal("27"), new BigDecimal("11"),
			new BigDecimal("46"), new BigDecimal("88"), new BigDecimal("24"), new BigDecimal("7"), new BigDecimal("62"),
			new BigDecimal("96"), new BigDecimal("51"), new BigDecimal("74"), new BigDecimal("61"),
			new BigDecimal("96"), new BigDecimal("6"), new BigDecimal("60"), new BigDecimal("60"), new BigDecimal("96"),
			new BigDecimal("73"), new BigDecimal("59"), new BigDecimal("33"), new BigDecimal("61"),
			new BigDecimal("29"), new BigDecimal("16"), new BigDecimal("96"), new BigDecimal("75"),
			new BigDecimal("73"), new BigDecimal("18"), new BigDecimal("79"), new BigDecimal("1"), new BigDecimal("94"),
			new BigDecimal("15"), new BigDecimal("67"), new BigDecimal("22"), new BigDecimal("82"),
			new BigDecimal("14"), new BigDecimal("60"), new BigDecimal("69"), new BigDecimal("62"),
			new BigDecimal("12"), new BigDecimal("61"), new BigDecimal("82"), new BigDecimal("34"),
			new BigDecimal("72"), new BigDecimal("68"), new BigDecimal("72"), new BigDecimal("18"), new BigDecimal("7"),
			new BigDecimal("37"), new BigDecimal("16"), new BigDecimal("2"), new BigDecimal("50"), new BigDecimal("71"),
			new BigDecimal("68"), new BigDecimal("26"), new BigDecimal("45"), new BigDecimal("95"),
			new BigDecimal("82"), new BigDecimal("92"), new BigDecimal("24"), new BigDecimal("66"),
			new BigDecimal("92"), new BigDecimal("92"), new BigDecimal("57"), new BigDecimal("41"),
			new BigDecimal("60"), new BigDecimal("62"), new BigDecimal("61"), new BigDecimal("92"), new BigDecimal("0"),
			new BigDecimal("39"), new BigDecimal("98"), new BigDecimal("12"), new BigDecimal("99"),
			new BigDecimal("38"), new BigDecimal("31"), new BigDecimal("6"), new BigDecimal("8"), new BigDecimal("18"),
			new BigDecimal("954"), new BigDecimal("598"), new BigDecimal("453"), new BigDecimal("982"),
			new BigDecimal("918"), new BigDecimal("345"), new BigDecimal("529"), new BigDecimal("8"),
			new BigDecimal("928"), new BigDecimal("591"), new BigDecimal("726"), new BigDecimal("23"),
			new BigDecimal("368"), new BigDecimal("744"), new BigDecimal("873"), new BigDecimal("307"),
			new BigDecimal("890"), new BigDecimal("650"), new BigDecimal("716"), new BigDecimal("175"),
			new BigDecimal("260"), new BigDecimal("772"), new BigDecimal("362"), new BigDecimal("487"),
			new BigDecimal("52"), new BigDecimal("794"), new BigDecimal("172"), new BigDecimal("516"),
			new BigDecimal("918"), new BigDecimal("222"), new BigDecimal("700"), new BigDecimal("830"),
			new BigDecimal("194"), new BigDecimal("663"), new BigDecimal("654"), new BigDecimal("573"),
			new BigDecimal("764"), new BigDecimal("209"), new BigDecimal("30"), new BigDecimal("395"),
			new BigDecimal("869"), new BigDecimal("129"), new BigDecimal("972"), new BigDecimal("177"),
			new BigDecimal("407"), new BigDecimal("938"), new BigDecimal("692"), new BigDecimal("637"),
			new BigDecimal("806"), new BigDecimal("497"), new BigDecimal("658"), new BigDecimal("157"),
			new BigDecimal("378"), new BigDecimal("625"), new BigDecimal("323"), new BigDecimal("859"),
			new BigDecimal("161"), new BigDecimal("620"), new BigDecimal("463"), new BigDecimal("668"),
			new BigDecimal("617"), new BigDecimal("807"), new BigDecimal("651"), new BigDecimal("174"),
			new BigDecimal("219"), new BigDecimal("265"), new BigDecimal("995"), new BigDecimal("366"),
			new BigDecimal("955"), new BigDecimal("344"), new BigDecimal("348"), new BigDecimal("652"),
			new BigDecimal("300"), new BigDecimal("792"), new BigDecimal("382"), new BigDecimal("771"),
			new BigDecimal("913"), new BigDecimal("888"), new BigDecimal("665"), new BigDecimal("809"),
			new BigDecimal("829"), new BigDecimal("282"), new BigDecimal("311"), new BigDecimal("295"),
			new BigDecimal("284"), new BigDecimal("383"), new BigDecimal("615"), new BigDecimal("821"),
			new BigDecimal("131"), new BigDecimal("181"), new BigDecimal("530"), new BigDecimal("6"),
			new BigDecimal("610"), new BigDecimal("191"), new BigDecimal("310"), new BigDecimal("83"),
			new BigDecimal("362"), new BigDecimal("215"), new BigDecimal("376"), new BigDecimal("950"),
			new BigDecimal("680"), new BigDecimal("106"), new BigDecimal("798"), new BigDecimal("533"),
			new BigDecimal("873"), new BigDecimal("808"), new BigDecimal("187"), new BigDecimal("945"),
			new BigDecimal("593"), new BigDecimal("174"), new BigDecimal("59"), new BigDecimal("67"),
			new BigDecimal("292"), new BigDecimal("474"), new BigDecimal("614"), new BigDecimal("2"),
			new BigDecimal("877"), new BigDecimal("291"), new BigDecimal("810"), new BigDecimal("979"),
			new BigDecimal("452"), new BigDecimal("58"), new BigDecimal("848"), new BigDecimal("341"),
			new BigDecimal("538"), new BigDecimal("656"), new BigDecimal("649"), new BigDecimal("903"),
			new BigDecimal("201"), new BigDecimal("326"), new BigDecimal("770"), new BigDecimal("369"),
			new BigDecimal("615"), new BigDecimal("763"), new BigDecimal("194"), new BigDecimal("20"),
			new BigDecimal("617"), new BigDecimal("334"), new BigDecimal("940"), new BigDecimal("503"),
			new BigDecimal("3"), new BigDecimal("400"), new BigDecimal("943"), new BigDecimal("455"),
			new BigDecimal("45"), new BigDecimal("748"), new BigDecimal("115"), new BigDecimal("697"),
			new BigDecimal("834"), new BigDecimal("366"), new BigDecimal("215"), new BigDecimal("407"),
			new BigDecimal("722"), new BigDecimal("486"), new BigDecimal("390"), new BigDecimal("222"),
			new BigDecimal("943"), new BigDecimal("640"), new BigDecimal("79"), new BigDecimal("272"),
			new BigDecimal("973"), new BigDecimal("689"), new BigDecimal("435"), new BigDecimal("618"),
			new BigDecimal("939"), new BigDecimal("967"), new BigDecimal("378"), new BigDecimal("132"),
			new BigDecimal("374"), new BigDecimal("965"), new BigDecimal("686"), new BigDecimal("328"),
			new BigDecimal("324"), new BigDecimal("450"), new BigDecimal("505"), new BigDecimal("41"),
			new BigDecimal("501"), new BigDecimal("45"), new BigDecimal("483"), new BigDecimal("609"),
			new BigDecimal("278"), new BigDecimal("359"), new BigDecimal("307"), new BigDecimal("39"),
			new BigDecimal("728"), new BigDecimal("749"), new BigDecimal("102"), new BigDecimal("412"),
			new BigDecimal("485"), new BigDecimal("513"), new BigDecimal("102"), new BigDecimal("480"),
			new BigDecimal("34"), new BigDecimal("436"), new BigDecimal("466"), new BigDecimal("566"),
			new BigDecimal("824"), new BigDecimal("874"), new BigDecimal("771"), new BigDecimal("513"),
			new BigDecimal("3829"), new BigDecimal("4769"), new BigDecimal("7270"), new BigDecimal("7857"),
			new BigDecimal("7840"), new BigDecimal("1671"), new BigDecimal("4653"), new BigDecimal("2781"),
			new BigDecimal("7553"), new BigDecimal("3904"), new BigDecimal("4201"), new BigDecimal("1686"),
			new BigDecimal("3966"), new BigDecimal("2313"), new BigDecimal("8842"), new BigDecimal("2798"),
			new BigDecimal("1850"), new BigDecimal("3652"), new BigDecimal("5214"), new BigDecimal("4468"),
			new BigDecimal("9895"), new BigDecimal("3152"), new BigDecimal("8496"), new BigDecimal("7581"),
			new BigDecimal("8893"), new BigDecimal("1508"), new BigDecimal("5272"), new BigDecimal("7009"),
			new BigDecimal("5587"), new BigDecimal("8440"), new BigDecimal("2449"), new BigDecimal("511"),
			new BigDecimal("1370"), new BigDecimal("4719"), new BigDecimal("5031"), new BigDecimal("8093"),
			new BigDecimal("7946"), new BigDecimal("6517"), new BigDecimal("143"), new BigDecimal("7904"),
			new BigDecimal("2003"), new BigDecimal("5058"), new BigDecimal("4993"), new BigDecimal("8612"),
			new BigDecimal("917"), new BigDecimal("2280"), new BigDecimal("6304"), new BigDecimal("7944"),
			new BigDecimal("7563"), new BigDecimal("8318"), new BigDecimal("3834"), new BigDecimal("3402"),
			new BigDecimal("1906"), new BigDecimal("3337"), new BigDecimal("7306"), new BigDecimal("7755"),
			new BigDecimal("7944"), new BigDecimal("5716"), new BigDecimal("4107"), new BigDecimal("6814"),
			new BigDecimal("4032"), new BigDecimal("1772"), new BigDecimal("2119"), new BigDecimal("546"),
			new BigDecimal("8354"), new BigDecimal("9179"), new BigDecimal("5470"), new BigDecimal("8793"),
			new BigDecimal("3834"), new BigDecimal("3402"), new BigDecimal("1906"), new BigDecimal("3337"),
			new BigDecimal("7306"), new BigDecimal("7755"), new BigDecimal("7944"), new BigDecimal("5716"),
			new BigDecimal("4107"), new BigDecimal("6814"), new BigDecimal("3834"), new BigDecimal("3402"),
			new BigDecimal("1906"), new BigDecimal("3337"), new BigDecimal("7306"), new BigDecimal("7755"),
			new BigDecimal("7944"), new BigDecimal("5716"), new BigDecimal("4107"), new BigDecimal("6814"),
			new BigDecimal("974"), new BigDecimal("4815"), new BigDecimal("6578"), new BigDecimal("6356"),
			new BigDecimal("5713"), new BigDecimal("9541"), new BigDecimal("3375"), new BigDecimal("8398"),
			new BigDecimal("6096"), new BigDecimal("1831"), new BigDecimal("1976"), new BigDecimal("9762"),
			new BigDecimal("6849"), new BigDecimal("1760"), new BigDecimal("7372"), new BigDecimal("2793"),
			new BigDecimal("6314"), new BigDecimal("2824"), new BigDecimal("5144"), new BigDecimal("9988"),
			new BigDecimal("1905"), new BigDecimal("3761"), new BigDecimal("1750"), new BigDecimal("2242"),
			new BigDecimal("8386"), new BigDecimal("7548"), new BigDecimal("6339"), new BigDecimal("9326"),
			new BigDecimal("2687"), new BigDecimal("3384"), new BigDecimal("6830"), new BigDecimal("5897"),
			new BigDecimal("1489"), new BigDecimal("2552"), new BigDecimal("3531"), new BigDecimal("831"),
			new BigDecimal("6237"), new BigDecimal("8192"), new BigDecimal("9992"), new BigDecimal("3321"),
			new BigDecimal("2482"), new BigDecimal("4124"), new BigDecimal("5351"), new BigDecimal("4510"),
			new BigDecimal("5535"), new BigDecimal("6957"), new BigDecimal("1914"), new BigDecimal("9638"),
			new BigDecimal("610"), new BigDecimal("5713"), new BigDecimal("8157"), new BigDecimal("4317"),
			new BigDecimal("170"), new BigDecimal("7373"), new BigDecimal("6893"), new BigDecimal("6593"),
			new BigDecimal("3614"), new BigDecimal("3834"), new BigDecimal("9383"), new BigDecimal("9832"),
			new BigDecimal("4278"), new BigDecimal("2202"), new BigDecimal("8059"), new BigDecimal("7664"),
			new BigDecimal("7639"), new BigDecimal("3237"), new BigDecimal("8350"), new BigDecimal("964"),
			new BigDecimal("3661"), new BigDecimal("1236"), new BigDecimal("1855"), new BigDecimal("8488"),
			new BigDecimal("1600"), new BigDecimal("1465"), new BigDecimal("5569"), new BigDecimal("962"),
			new BigDecimal("1417"), new BigDecimal("6057"), new BigDecimal("2055"), new BigDecimal("4997"),
			new BigDecimal("4510"), new BigDecimal("248"), new BigDecimal("5855"), new BigDecimal("3323"),
			new BigDecimal("7225"), new BigDecimal("751"), new BigDecimal("917"), new BigDecimal("7454"),
			new BigDecimal("5319"), new BigDecimal("5500"), new BigDecimal("8840"), new BigDecimal("3427"),
			new BigDecimal("6396"), new BigDecimal("7718"), new BigDecimal("3337"), new BigDecimal("3283"),
			new BigDecimal("8979"), new BigDecimal("5418"), new BigDecimal("3327"), new BigDecimal("9239"),
			new BigDecimal("1571"), new BigDecimal("7271"), new BigDecimal("6408"), new BigDecimal("8752"),
			new BigDecimal("5198"), new BigDecimal("4676"), new BigDecimal("8138"), new BigDecimal("5665"),
			new BigDecimal("9907"), new BigDecimal("9849"), new BigDecimal("2744"), new BigDecimal("6137"),
			new BigDecimal("2755"), new BigDecimal("8928"), new BigDecimal("8000"), new BigDecimal("1425"),
			new BigDecimal("2048"), new BigDecimal("185"), new BigDecimal("5361"), new BigDecimal("3022"),
			new BigDecimal("4193"), new BigDecimal("3860"), new BigDecimal("9785"), new BigDecimal("867"),
			new BigDecimal("3891"), new BigDecimal("7743"), new BigDecimal("6684"), new BigDecimal("550"),
			new BigDecimal("6874"), new BigDecimal("9836"), new BigDecimal("1554"), new BigDecimal("2589"),
			new BigDecimal("39285"), new BigDecimal("67053"), new BigDecimal("64215"), new BigDecimal("25493"),
			new BigDecimal("49807"), new BigDecimal("95711"), new BigDecimal("81334"), new BigDecimal("88735"),
			new BigDecimal("54753"), new BigDecimal("80832"), new BigDecimal("89535"), new BigDecimal("79607"),
			new BigDecimal("76932"), new BigDecimal("26335"), new BigDecimal("55013"), new BigDecimal("94307"),
			new BigDecimal("30501"), new BigDecimal("17279"), new BigDecimal("75973"), new BigDecimal("98388"),
			new BigDecimal("95074"), new BigDecimal("51447"), new BigDecimal("30752"), new BigDecimal("78140"),
			new BigDecimal("72689"), new BigDecimal("39913"), new BigDecimal("78729"), new BigDecimal("95948"),
			new BigDecimal("59551"), new BigDecimal("53437"), new BigDecimal("19898"), new BigDecimal("50696"),
			new BigDecimal("8213"), new BigDecimal("98009"), new BigDecimal("53473"), new BigDecimal("75914"),
			new BigDecimal("91031"), new BigDecimal("1727"), new BigDecimal("34232"), new BigDecimal("41616"),
			new BigDecimal("97287"), new BigDecimal("64675"), new BigDecimal("58962"), new BigDecimal("40323"),
			new BigDecimal("72742"), new BigDecimal("94855"), new BigDecimal("63441"), new BigDecimal("78789"),
			new BigDecimal("38298"), new BigDecimal("54997"), new BigDecimal("44213"), new BigDecimal("2550"),
			new BigDecimal("61227"), new BigDecimal("55353"), new BigDecimal("8335"), new BigDecimal("90850"),
			new BigDecimal("97896"), new BigDecimal("66713"), new BigDecimal("66259"), new BigDecimal("61098"),
			new BigDecimal("80679"), new BigDecimal("35857"), new BigDecimal("30796"), new BigDecimal("17422"),
			new BigDecimal("45833"), new BigDecimal("72751"), new BigDecimal("99369"), new BigDecimal("25044"),
			new BigDecimal("72603"), new BigDecimal("6056"), new BigDecimal("19829"), new BigDecimal("25240"),
			new BigDecimal("94265"), new BigDecimal("74325"), new BigDecimal("50149"), new BigDecimal("63473"),
			new BigDecimal("33581"), new BigDecimal("9070"), new BigDecimal("15882"), new BigDecimal("71195"),
			new BigDecimal("5205"), new BigDecimal("56609"), new BigDecimal("77094"), new BigDecimal("94309"),
			new BigDecimal("20675"), new BigDecimal("95822"), new BigDecimal("22523"), new BigDecimal("31545"),
			new BigDecimal("62366"), new BigDecimal("57026"), new BigDecimal("20287"), new BigDecimal("89028"),
			new BigDecimal("49765"), new BigDecimal("20878"), new BigDecimal("6444"), new BigDecimal("23266"),
			new BigDecimal("34740"), new BigDecimal("196"), new BigDecimal("5396"), new BigDecimal("52007"),
			new BigDecimal("62463"), new BigDecimal("51566"), new BigDecimal("41681"), new BigDecimal("1228"),
			new BigDecimal("19899"), new BigDecimal("33450"), new BigDecimal("45594"), new BigDecimal("81492"),
			new BigDecimal("2268"), new BigDecimal("82502"), new BigDecimal("40434"), new BigDecimal("49561"),
			new BigDecimal("56985"), new BigDecimal("42740"), new BigDecimal("84603"), new BigDecimal("62680"),
			new BigDecimal("46796"), new BigDecimal("48010"), new BigDecimal("50920"), new BigDecimal("49238"),
			new BigDecimal("30229"), new BigDecimal("85913"), new BigDecimal("74192"), new BigDecimal("54797"),
			new BigDecimal("31614"), new BigDecimal("88455"), new BigDecimal("11933"), new BigDecimal("52024"),
			new BigDecimal("85657"), new BigDecimal("85963"), new BigDecimal("98564"), new BigDecimal("75351"),
			new BigDecimal("15366"), new BigDecimal("29989"), new BigDecimal("75632"), new BigDecimal("68766"),
			new BigDecimal("62705"), new BigDecimal("38071"), new BigDecimal("73079"), new BigDecimal("93856"),
			new BigDecimal("62744"), new BigDecimal("93156"), new BigDecimal("23790"), new BigDecimal("20820"),
			new BigDecimal("11401"), new BigDecimal("516"), new BigDecimal("52632"), new BigDecimal("29825"),
			new BigDecimal("91786"), new BigDecimal("72513"), new BigDecimal("31673"), new BigDecimal("12940"),
			new BigDecimal("6291"), new BigDecimal("95377"), new BigDecimal("79434"), new BigDecimal("15130"),
			new BigDecimal("85717"), new BigDecimal("15504"), new BigDecimal("3493"), new BigDecimal("46596"),
			new BigDecimal("54590"), new BigDecimal("39161"), new BigDecimal("63871"), new BigDecimal("4535"),
			new BigDecimal("97108"), new BigDecimal("19040"), new BigDecimal("85877"), new BigDecimal("71012"),
			new BigDecimal("38303"), new BigDecimal("26448"), new BigDecimal("53343"), new BigDecimal("59243"),
			new BigDecimal("69092"), new BigDecimal("82236"), new BigDecimal("15618"), new BigDecimal("52766"),
			new BigDecimal("33739"), new BigDecimal("68100"), new BigDecimal("18082"), new BigDecimal("61077"),
			new BigDecimal("3714"), new BigDecimal("3654"), new BigDecimal("74484"), new BigDecimal("17134"),
			new BigDecimal("63098"), new BigDecimal("14302"), new BigDecimal("22289"), new BigDecimal("76991"),
			new BigDecimal("11183"), new BigDecimal("81724"), new BigDecimal("14726"), new BigDecimal("10746"),
			new BigDecimal("23110"), new BigDecimal("32097"), new BigDecimal("40776"), new BigDecimal("54939"),
			new BigDecimal("60687"), new BigDecimal("17397"), new BigDecimal("95302"), new BigDecimal("68113"),
			MAX_VALUE, MAX_VALUE, MAX_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE };
}
