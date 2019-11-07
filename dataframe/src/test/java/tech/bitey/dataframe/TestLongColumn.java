package tech.bitey.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TestLongColumn extends TestColumn<Long> {

	private static final Random RAND = new Random(0);
	
	@Override
	TestSample<Long> wrapSample(String label, Long[] array, int characteristics) {
		LongColumn column = LongColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}
	
	@Override
	TestSample<Long> wrapSample(String label, Long[] array, Column<Long> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	
	@Override
	Long[] toArray(Collection<Long> samples) {
		return samples.toArray(empty());
	}
	
	@Override
	Long[] empty() {
		return new Long[0];
	}

	@Override
	Long[] singleNull() {
		return new Long[] { null };
	}

	@Override
	Long[] singleNonNull() {
		return new Long[] { 0L };
	}

	@Override
	Long[] duoFirstNull() {
		return new Long[] { null, 0L };
	}

	@Override
	Long[] duoBothNull() {
		return new Long[] { null, null };
	}

	@Override
	Long[] duoDistinct() {
		return new Long[] { 0L, 1L };
	}

	@Override
	Long[] duoSame() {
		return new Long[] { 0L, 0L };
	}

	@Override
	Long[] minMax() {
		return new Long[] { Long.MIN_VALUE, Long.MAX_VALUE };
	}

	@Override
	Long[] allNull(int size) {
		return new Long[size];
	}

	@Override
	Long[] random(int size) {
		List<Long> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new Long[0]);
	}

	@Override
	Long[] NXNX(int size) {
		Long[] random = random(size);
		for(int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	Long[] NNXX(int size) {
		Long[] random = random(size);
		for(int i = 0; i < size; i += 4) {
			random[i] = null;
			if(i+1 < size)
				random[i+1] = null;
		}
		return random;
	}

	@Override
	Long[] sequential(int size) {
		Long[] elements = new Long[size];
		for (int i = 0; i < size; i++)
			elements[i] = (long)i;
		return elements;
	}

	@Override
	Long[] same(int size) {
		Long[] elements = new Long[size];
		for (int i = 0; i < size; i++)
			elements[i] = 0L;
		return elements;
	}

	@Override
	Long[] smar(int size) {
		Long[] elements = new Long[size];
		long n = 1;
		for (int i = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = n;
		return elements;
	}
	
	@Override
	Long[] notPresent() {
		return new Long[] {-2378457L, -347700L, -88L};
	}
	
	// 1026 values
	private static final Long[] RANDOM = { 5L, 5L, 8L, 5L, 2L, 6L, 9L, 2L, 2L, 8L, 7L, 1L, 5L, 1L, 3L, 4L, 8L, 8L, 3L, 1L, 5L, 6L, 1L, 2L, 1L,
			0L, 1L, 4L, 5L, 9L, 1L, 2L, 2L, 9L, 7L, 8L, 8L, 6L, 2L, 0L, 3L, 9L, 6L, 2L, 6L, 5L, 9L, 8L, 8L, 6L, 3L, 8L, 9L, 3L, 8L, 7L, 2L, 1L, 5L, 9L, 2L,
			2L, 5L, 0L, 6L, 1L, 8L, 8L, 6L, 0L, 1L, 8L, 6L, 2L, 2L, 4L, 1L, 0L, 7L, 4L, 3L, 1L, 3L, 3L, 4L, 0L, 7L, 4L, 7L, 9L, 6L, 9L, 5L, 6L, 6L, 8L, 1L,
			2L, 7L, 2L, 3L, 0L, 4L, 2L, 5L, 6L, 2L, 7L, 1L, 5L, 3L, 3L, 4L, 8L, 8L, 3L, 2L, 5L, 6L, 7L, 6L, 0L, 3L, 7L, 0L, 3L, 9L, 8L, 6L, 1L, 5L, 3L, 6L,
			4L, 3L, 4L, 3L, 2L, 9L, 6L, 3L, 7L, 6L, 3L, 1L, 8L, 7L, 2L, 0L, 1L, 6L, 2L, 4L, 5L, 7L, 4L, 1L, 2L, 4L, 8L, 7L, 2L, 3L, 7L, 4L, 1L, 1L, 1L, 8L,
			9L, 5L, 1L, 2L, 2L, 3L, 0L, 1L, 8L, 9L, 9L, 0L, 2L, 1L, 2L, 3L, 2L, 0L, 4L, 5L, 2L, 9L, 6L, 7L, 6L, 1L, 3L, 0L, 9L, 5L, 5L, 41L, 41L, 14L, 48L,
			23L, 48L, 2L, 93L, 46L, 62L, 70L, 82L, 27L, 80L, 31L, 85L, 64L, 51L, 48L, 37L, 68L, 17L, 49L, 95L, 0L, 2L, 37L, 88L, 61L, 88L, 59L, 90L,
			75L, 23L, 55L, 0L, 15L, 36L, 91L, 96L, 84L, 53L, 81L, 32L, 12L, 15L, 27L, 57L, 85L, 8L, 2L, 13L, 44L, 60L, 53L, 40L, 70L, 75L, 70L, 89L,
			6L, 47L, 35L, 90L, 72L, 53L, 3L, 42L, 0L, 87L, 0L, 35L, 39L, 49L, 40L, 54L, 70L, 44L, 56L, 47L, 72L, 75L, 97L, 93L, 62L, 51L, 19L, 26L,
			71L, 98L, 1L, 36L, 23L, 60L, 94L, 14L, 21L, 97L, 14L, 94L, 84L, 27L, 40L, 87L, 87L, 28L, 12L, 62L, 22L, 37L, 42L, 37L, 73L, 23L, 74L,
			77L, 27L, 29L, 83L, 19L, 60L, 3L, 10L, 27L, 11L, 46L, 88L, 24L, 7L, 62L, 96L, 51L, 74L, 61L, 96L, 6L, 60L, 60L, 96L, 73L, 59L, 33L, 61L,
			29L, 16L, 96L, 75L, 73L, 18L, 79L, 1L, 94L, 15L, 67L, 22L, 82L, 14L, 60L, 69L, 62L, 12L, 61L, 82L, 34L, 72L, 68L, 72L, 18L, 7L, 37L,
			16L, 2L, 50L, 71L, 68L, 26L, 45L, 95L, 82L, 92L, 24L, 66L, 92L, 92L, 57L, 41L, 60L, 62L, 61L, 92L, 0L, 39L, 98L, 12L, 99L, 38L, 31L, 6L,
			8L, 18L, 954L, 598L, 453L, 982L, 918L, 345L, 529L, 8L, 928L, 591L, 726L, 23L, 368L, 744L, 873L, 307L, 890L, 650L, 716L, 175L, 260L,
			772L, 362L, 487L, 52L, 794L, 172L, 516L, 918L, 222L, 700L, 830L, 194L, 663L, 654L, 573L, 764L, 209L, 30L, 395L, 869L, 129L, 972L,
			177L, 407L, 938L, 692L, 637L, 806L, 497L, 658L, 157L, 378L, 625L, 323L, 859L, 161L, 620L, 463L, 668L, 617L, 807L, 651L, 174L,
			219L, 265L, 995L, 366L, 955L, 344L, 348L, 652L, 300L, 792L, 382L, 771L, 913L, 888L, 665L, 809L, 829L, 282L, 311L, 295L, 284L,
			383L, 615L, 821L, 131L, 181L, 530L, 6L, 610L, 191L, 310L, 83L, 362L, 215L, 376L, 950L, 680L, 106L, 798L, 533L, 873L, 808L, 187L,
			945L, 593L, 174L, 59L, 67L, 292L, 474L, 614L, 2L, 877L, 291L, 810L, 979L, 452L, 58L, 848L, 341L, 538L, 656L, 649L, 903L, 201L,
			326L, 770L, 369L, 615L, 763L, 194L, 20L, 617L, 334L, 940L, 503L, 3L, 400L, 943L, 455L, 45L, 748L, 115L, 697L, 834L, 366L, 215L,
			407L, 722L, 486L, 390L, 222L, 943L, 640L, 79L, 272L, 973L, 689L, 435L, 618L, 939L, 967L, 378L, 132L, 374L, 965L, 686L, 328L, 324L,
			450L, 505L, 41L, 501L, 45L, 483L, 609L, 278L, 359L, 307L, 39L, 728L, 749L, 102L, 412L, 485L, 513L, 102L, 480L, 34L, 436L, 466L,
			566L, 824L, 874L, 771L, 513L, 3829L, 4769L, 7270L, 7857L, 7840L, 1671L, 4653L, 2781L, 7553L, 3904L, 4201L, 1686L, 3966L, 2313L,
			8842L, 2798L, 1850L, 3652L, 5214L, 4468L, 9895L, 3152L, 8496L, 7581L, 8893L, 1508L, 5272L, 7009L, 5587L, 8440L, 2449L, 511L,
			1370L, 4719L, 5031L, 8093L, 7946L, 6517L, 143L, 7904L, 2003L, 5058L, 4993L, 8612L, 917L, 2280L, 6304L, 7944L, 7563L, 8318L,
			3834L, 3402L, 1906L, 3337L, 7306L, 7755L, 7944L, 5716L, 4107L, 6814L, 4032L, 1772L, 2119L, 546L, 8354L, 9179L, 5470L, 8793L,
			3834L, 3402L, 1906L, 3337L, 7306L, 7755L, 7944L, 5716L, 4107L, 6814L, 3834L, 3402L, 1906L, 3337L, 7306L, 7755L, 7944L, 5716L,
			4107L, 6814L, 974L, 4815L, 6578L, 6356L, 5713L, 9541L, 3375L, 8398L, 6096L, 1831L, 1976L, 9762L, 6849L, 1760L, 7372L, 2793L,
			6314L, 2824L, 5144L, 9988L, 1905L, 3761L, 1750L, 2242L, 8386L, 7548L, 6339L, 9326L, 2687L, 3384L, 6830L, 5897L, 1489L, 2552L,
			3531L, 831L, 6237L, 8192L, 9992L, 3321L, 2482L, 4124L, 5351L, 4510L, 5535L, 6957L, 1914L, 9638L, 610L, 5713L, 8157L, 4317L,
			170L, 7373L, 6893L, 6593L, 3614L, 3834L, 9383L, 9832L, 4278L, 2202L, 8059L, 7664L, 7639L, 3237L, 8350L, 964L, 3661L, 1236L,
			1855L, 8488L, 1600L, 1465L, 5569L, 962L, 1417L, 6057L, 2055L, 4997L, 4510L, 248L, 5855L, 3323L, 7225L, 751L, 917L, 7454L,
			5319L, 5500L, 8840L, 3427L, 6396L, 7718L, 3337L, 3283L, 8979L, 5418L, 3327L, 9239L, 1571L, 7271L, 6408L, 8752L, 5198L, 4676L,
			8138L, 5665L, 9907L, 9849L, 2744L, 6137L, 2755L, 8928L, 8000L, 1425L, 2048L, 185L, 5361L, 3022L, 4193L, 3860L, 9785L, 867L,
			3891L, 7743L, 6684L, 550L, 6874L, 9836L, 1554L, 2589L, 39285L, 67053L, 64215L, 25493L, 49807L, 95711L, 81334L, 88735L,
			54753L, 80832L, 89535L, 79607L, 76932L, 26335L, 55013L, 94307L, 30501L, 17279L, 75973L, 98388L, 95074L, 51447L, 30752L,
			78140L, 72689L, 39913L, 78729L, 95948L, 59551L, 53437L, 19898L, 50696L, 8213L, 98009L, 53473L, 75914L, 91031L, 1727L,
			34232L, 41616L, 97287L, 64675L, 58962L, 40323L, 72742L, 94855L, 63441L, 78789L, 38298L, 54997L, 44213L, 2550L, 61227L,
			55353L, 8335L, 90850L, 97896L, 66713L, 66259L, 61098L, 80679L, 35857L, 30796L, 17422L, 45833L, 72751L, 99369L, 25044L,
			72603L, 6056L, 19829L, 25240L, 94265L, 74325L, 50149L, 63473L, 33581L, 9070L, 15882L, 71195L, 5205L, 56609L, 77094L, 94309L,
			20675L, 95822L, 22523L, 31545L, 62366L, 57026L, 20287L, 89028L, 49765L, 20878L, 6444L, 23266L, 34740L, 196L, 5396L, 52007L,
			62463L, 51566L, 41681L, 1228L, 19899L, 33450L, 45594L, 81492L, 2268L, 82502L, 40434L, 49561L, 56985L, 42740L, 84603L,
			62680L, 46796L, 48010L, 50920L, 49238L, 30229L, 85913L, 74192L, 54797L, 31614L, 88455L, 11933L, 52024L, 85657L, 85963L,
			98564L, 75351L, 15366L, 29989L, 75632L, 68766L, 62705L, 38071L, 73079L, 93856L, 62744L, 93156L, 23790L, 20820L, 11401L,
			516L, 52632L, 29825L, 91786L, 72513L, 31673L, 12940L, 6291L, 95377L, 79434L, 15130L, 85717L, 15504L, 3493L, 46596L, 54590L,
			39161L, 63871L, 4535L, 97108L, 19040L, 85877L, 71012L, 38303L, 26448L, 53343L, 59243L, 69092L, 82236L, 15618L, 52766L,
			33739L, 68100L, 18082L, 61077L, 3714L, 3654L, 74484L, 17134L, 63098L, 14302L, 22289L, 76991L, 11183L, 81724L, 14726L,
			10746L, 23110L, 32097L, 40776L, 54939L, 60687L, 17397L, 95302L, 68113L, Long.MAX_VALUE, Long.MAX_VALUE,
			Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE };
}
