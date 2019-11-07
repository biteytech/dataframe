package tech.bitey.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TestFloatColumn extends TestFloatingColumn<Float> {

	private static final Random RAND = new Random(0);
	
	@Override
	TestSample<Float> wrapSample(String label, Float[] array, int characteristics) {
		FloatColumn column = FloatColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}
	
	@Override
	TestSample<Float> wrapSample(String label, Float[] array, Column<Float> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	
	@Override
	Float[] toArray(Collection<Float> samples) {
		return samples.toArray(empty());
	}
	
	@Override
	Float[] singleNaN() {
		return new Float[] {Float.NaN};
	}

	@Override
	Float[] duoNaN() {
		return new Float[] {Float.NaN, Float.NaN};
	}

	@Override
	Float[] nonFinite() {
		return new Float[] {Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NaN};
	}
	
	@Override
	Float[] empty() {
		return new Float[0];
	}

	@Override
	Float[] singleNull() {
		return new Float[] { null };
	}

	@Override
	Float[] singleNonNull() {
		return new Float[] { 0F };
	}

	@Override
	Float[] duoFirstNull() {
		return new Float[] { null, 0F };
	}

	@Override
	Float[] duoBothNull() {
		return new Float[] { null, null };
	}

	@Override
	Float[] duoDistinct() {
		return new Float[] { 0F, 1F };
	}

	@Override
	Float[] duoSame() {
		return new Float[] { 0F, 0F };
	}

	@Override
	Float[] minMax() {
		return new Float[] { Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY };
	}

	@Override
	Float[] allNull(int size) {
		return new Float[size];
	}

	@Override
	Float[] random(int size) {
		List<Float> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new Float[0]);
	}

	@Override
	Float[] NXNX(int size) {
		Float[] random = random(size);
		for(int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	Float[] NNXX(int size) {
		Float[] random = random(size);
		for(int i = 0; i < size; i += 4) {
			random[i] = null;
			if(i+1 < size)
				random[i+1] = null;
		}
		return random;
	}

	@Override
	Float[] sequential(int size) {
		Float[] elements = new Float[size];
		for (int i = 0; i < size; i++)
			elements[i] = (float)i;
		return elements;
	}

	@Override
	Float[] same(int size) {
		Float[] elements = new Float[size];
		for (int i = 0; i < size; i++)
			elements[i] = 0F;
		return elements;
	}

	@Override
	Float[] smar(int size) {
		Float[] elements = new Float[size];
		float n = 1;
		for (int i = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = n;
		return elements;
	}
	
	@Override
	Float[] notPresent() {
		return new Float[] {-2378457F, -347700F, -88F};
	}
	
	// 1026 values
	private static final Float[] RANDOM = { 5F, 5F, 8F, 5F, 2F, 6F, 9F, 2F, 2F, 8F, 7F, 1F, 5F, 1F, 3F, 4F, 8F, 8F, 3F, 1F, 5F, 6F, 1F, 2F, 1F,
			0F, 1F, 4F, 5F, 9F, 1F, 2F, 2F, 9F, 7F, 8F, 8F, 6F, 2F, 0F, 3F, 9F, 6F, 2F, 6F, 5F, 9F, 8F, 8F, 6F, 3F, 8F, 9F, 3F, 8F, 7F, 2F, 1F, 5F, 9F, 2F,
			2F, 5F, 0F, 6F, 1F, 8F, 8F, 6F, 0F, 1F, 8F, 6F, 2F, 2F, 4F, 1F, 0F, 7F, 4F, 3F, 1F, 3F, 3F, 4F, 0F, 7F, 4F, 7F, 9F, 6F, 9F, 5F, 6F, 6F, 8F, 1F,
			2F, 7F, 2F, 3F, 0F, 4F, 2F, 5F, 6F, 2F, 7F, 1F, 5F, 3F, 3F, 4F, 8F, 8F, 3F, 2F, 5F, 6F, 7F, 6F, 0F, 3F, 7F, 0F, 3F, 9F, 8F, 6F, 1F, 5F, 3F, 6F,
			4F, 3F, 4F, 3F, 2F, 9F, 6F, 3F, 7F, 6F, 3F, 1F, 8F, 7F, 2F, 0F, 1F, 6F, 2F, 4F, 5F, 7F, 4F, 1F, 2F, 4F, 8F, 7F, 2F, 3F, 7F, 4F, 1F, 1F, 1F, 8F,
			9F, 5F, 1F, 2F, 2F, 3F, 0F, 1F, 8F, 9F, 9F, 0F, 2F, 1F, 2F, 3F, 2F, 0F, 4F, 5F, 2F, 9F, 6F, 7F, 6F, 1F, 3F, 0F, 9F, 5F, 5F, 41F, 41F, 14F, 48F,
			23F, 48F, 2F, 93F, 46F, 62F, 70F, 82F, 27F, 80F, 31F, 85F, 64F, 51F, 48F, 37F, 68F, 17F, 49F, 95F, 0F, 2F, 37F, 88F, 61F, 88F, 59F, 90F,
			75F, 23F, 55F, 0F, 15F, 36F, 91F, 96F, 84F, 53F, 81F, 32F, 12F, 15F, 27F, 57F, 85F, 8F, 2F, 13F, 44F, 60F, 53F, 40F, 70F, 75F, 70F, 89F,
			6F, 47F, 35F, 90F, 72F, 53F, 3F, 42F, 0F, 87F, 0F, 35F, 39F, 49F, 40F, 54F, 70F, 44F, 56F, 47F, 72F, 75F, 97F, 93F, 62F, 51F, 19F, 26F,
			71F, 98F, 1F, 36F, 23F, 60F, 94F, 14F, 21F, 97F, 14F, 94F, 84F, 27F, 40F, 87F, 87F, 28F, 12F, 62F, 22F, 37F, 42F, 37F, 73F, 23F, 74F,
			77F, 27F, 29F, 83F, 19F, 60F, 3F, 10F, 27F, 11F, 46F, 88F, 24F, 7F, 62F, 96F, 51F, 74F, 61F, 96F, 6F, 60F, 60F, 96F, 73F, 59F, 33F, 61F,
			29F, 16F, 96F, 75F, 73F, 18F, 79F, 1F, 94F, 15F, 67F, 22F, 82F, 14F, 60F, 69F, 62F, 12F, 61F, 82F, 34F, 72F, 68F, 72F, 18F, 7F, 37F,
			16F, 2F, 50F, 71F, 68F, 26F, 45F, 95F, 82F, 92F, 24F, 66F, 92F, 92F, 57F, 41F, 60F, 62F, 61F, 92F, 0F, 39F, 98F, 12F, 99F, 38F, 31F, 6F,
			8F, 18F, 954F, 598F, 453F, 982F, 918F, 345F, 529F, 8F, 928F, 591F, 726F, 23F, 368F, 744F, 873F, 307F, 890F, 650F, 716F, 175F, 260F,
			772F, 362F, 487F, 52F, 794F, 172F, 516F, 918F, 222F, 700F, 830F, 194F, 663F, 654F, 573F, 764F, 209F, 30F, 395F, 869F, 129F, 972F,
			177F, 407F, 938F, 692F, 637F, 806F, 497F, 658F, 157F, 378F, 625F, 323F, 859F, 161F, 620F, 463F, 668F, 617F, 807F, 651F, 174F,
			219F, 265F, 995F, 366F, 955F, 344F, 348F, 652F, 300F, 792F, 382F, 771F, 913F, 888F, 665F, 809F, 829F, 282F, 311F, 295F, 284F,
			383F, 615F, 821F, 131F, 181F, 530F, 6F, 610F, 191F, 310F, 83F, 362F, 215F, 376F, 950F, 680F, 106F, 798F, 533F, 873F, 808F, 187F,
			945F, 593F, 174F, 59F, 67F, 292F, 474F, 614F, 2F, 877F, 291F, 810F, 979F, 452F, 58F, 848F, 341F, 538F, 656F, 649F, 903F, 201F,
			326F, 770F, 369F, 615F, 763F, 194F, 20F, 617F, 334F, 940F, 503F, 3F, 400F, 943F, 455F, 45F, 748F, 115F, 697F, 834F, 366F, 215F,
			407F, 722F, 486F, 390F, 222F, 943F, 640F, 79F, 272F, 973F, 689F, 435F, 618F, 939F, 967F, 378F, 132F, 374F, 965F, 686F, 328F, 324F,
			450F, 505F, 41F, 501F, 45F, 483F, 609F, 278F, 359F, 307F, 39F, 728F, 749F, 102F, 412F, 485F, 513F, 102F, 480F, 34F, 436F, 466F,
			566F, 824F, 874F, 771F, 513F, 3829F, 4769F, 7270F, 7857F, 7840F, 1671F, 4653F, 2781F, 7553F, 3904F, 4201F, 1686F, 3966F, 2313F,
			8842F, 2798F, 1850F, 3652F, 5214F, 4468F, 9895F, 3152F, 8496F, 7581F, 8893F, 1508F, 5272F, 7009F, 5587F, 8440F, 2449F, 511F,
			1370F, 4719F, 5031F, 8093F, 7946F, 6517F, 143F, 7904F, 2003F, 5058F, 4993F, 8612F, 917F, 2280F, 6304F, 7944F, 7563F, 8318F,
			3834F, 3402F, 1906F, 3337F, 7306F, 7755F, 7944F, 5716F, 4107F, 6814F, 4032F, 1772F, 2119F, 546F, 8354F, 9179F, 5470F, 8793F,
			3834F, 3402F, 1906F, 3337F, 7306F, 7755F, 7944F, 5716F, 4107F, 6814F, 3834F, 3402F, 1906F, 3337F, 7306F, 7755F, 7944F, 5716F,
			4107F, 6814F, 974F, 4815F, 6578F, 6356F, 5713F, 9541F, 3375F, 8398F, 6096F, 1831F, 1976F, 9762F, 6849F, 1760F, 7372F, 2793F,
			6314F, 2824F, 5144F, 9988F, 1905F, 3761F, 1750F, 2242F, 8386F, 7548F, 6339F, 9326F, 2687F, 3384F, 6830F, 5897F, 1489F, 2552F,
			3531F, 831F, 6237F, 8192F, 9992F, 3321F, 2482F, 4124F, 5351F, 4510F, 5535F, 6957F, 1914F, 9638F, 610F, 5713F, 8157F, 4317F,
			170F, 7373F, 6893F, 6593F, 3614F, 3834F, 9383F, 9832F, 4278F, 2202F, 8059F, 7664F, 7639F, 3237F, 8350F, 964F, 3661F, 1236F,
			1855F, 8488F, 1600F, 1465F, 5569F, 962F, 1417F, 6057F, 2055F, 4997F, 4510F, 248F, 5855F, 3323F, 7225F, 751F, 917F, 7454F,
			5319F, 5500F, 8840F, 3427F, 6396F, 7718F, 3337F, 3283F, 8979F, 5418F, 3327F, 9239F, 1571F, 7271F, 6408F, 8752F, 5198F, 4676F,
			8138F, 5665F, 9907F, 9849F, 2744F, 6137F, 2755F, 8928F, 8000F, 1425F, 2048F, 185F, 5361F, 3022F, 4193F, 3860F, 9785F, 867F,
			3891F, 7743F, 6684F, 550F, 6874F, 9836F, 1554F, 2589F, 39285F, 67053F, 64215F, 25493F, 49807F, 95711F, 81334F, 88735F,
			54753F, 80832F, 89535F, 79607F, 76932F, 26335F, 55013F, 94307F, 30501F, 17279F, 75973F, 98388F, 95074F, 51447F, 30752F,
			78140F, 72689F, 39913F, 78729F, 95948F, 59551F, 53437F, 19898F, 50696F, 8213F, 98009F, 53473F, 75914F, 91031F, 1727F,
			34232F, 41616F, 97287F, 64675F, 58962F, 40323F, 72742F, 94855F, 63441F, 78789F, 38298F, 54997F, 44213F, 2550F, 61227F,
			55353F, 8335F, 90850F, 97896F, 66713F, 66259F, 61098F, 80679F, 35857F, 30796F, 17422F, 45833F, 72751F, 99369F, 25044F,
			72603F, 6056F, 19829F, 25240F, 94265F, 74325F, 50149F, 63473F, 33581F, 9070F, 15882F, 71195F, 5205F, 56609F, 77094F, 94309F,
			20675F, 95822F, 22523F, 31545F, 62366F, 57026F, 20287F, 89028F, 49765F, 20878F, 6444F, 23266F, 34740F, 196F, 5396F, 52007F,
			62463F, 51566F, 41681F, 1228F, 19899F, 33450F, 45594F, 81492F, 2268F, 82502F, 40434F, 49561F, 56985F, 42740F, 84603F,
			62680F, 46796F, 48010F, 50920F, 49238F, 30229F, 85913F, 74192F, 54797F, 31614F, 88455F, 11933F, 52024F, 85657F, 85963F,
			98564F, 75351F, 15366F, 29989F, 75632F, 68766F, 62705F, 38071F, 73079F, 93856F, 62744F, 93156F, 23790F, 20820F, 11401F,
			516F, 52632F, 29825F, 91786F, 72513F, 31673F, 12940F, 6291F, 95377F, 79434F, 15130F, 85717F, 15504F, 3493F, 46596F, 54590F,
			39161F, 63871F, 4535F, 97108F, 19040F, 85877F, 71012F, 38303F, 26448F, 53343F, 59243F, 69092F, 82236F, 15618F, 52766F,
			33739F, 68100F, 18082F, 61077F, 3714F, 3654F, 74484F, 17134F, 63098F, 14302F, 22289F, 76991F, 11183F, 81724F, 14726F,
			10746F, 23110F, 32097F, 40776F, 54939F, 60687F, 17397F, 95302F, 68113F, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY,
			Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY };

}
