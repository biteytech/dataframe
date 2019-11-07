package tech.bitey.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TestDoubleColumn extends TestFloatingColumn<Double> {

	private static final Random RAND = new Random(0);
	
	@Override
	TestSample<Double> wrapSample(String label, Double[] array, int characteristics) {
		DoubleColumn column = DoubleColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}
	
	@Override
	TestSample<Double> wrapSample(String label, Double[] array, Column<Double> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	
	@Override
	Double[] toArray(Collection<Double> samples) {
		return samples.toArray(empty());
	}
	
	@Override
	Double[] singleNaN() {
		return new Double[] {Double.NaN};
	}

	@Override
	Double[] duoNaN() {
		return new Double[] {Double.NaN, Double.NaN};
	}

	@Override
	Double[] nonFinite() {
		return new Double[] {Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN};
	}
	
	@Override
	Double[] empty() {
		return new Double[0];
	}

	@Override
	Double[] singleNull() {
		return new Double[] { null };
	}

	@Override
	Double[] singleNonNull() {
		return new Double[] { 0D };
	}

	@Override
	Double[] duoFirstNull() {
		return new Double[] { null, 0D };
	}

	@Override
	Double[] duoBothNull() {
		return new Double[] { null, null };
	}

	@Override
	Double[] duoDistinct() {
		return new Double[] { 0D, 1D };
	}

	@Override
	Double[] duoSame() {
		return new Double[] { 0D, 0D };
	}

	@Override
	Double[] minMax() {
		return new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY };
	}

	@Override
	Double[] allNull(int size) {
		return new Double[size];
	}

	@Override
	Double[] random(int size) {
		List<Double> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new Double[0]);
	}

	@Override
	Double[] NXNX(int size) {
		Double[] random = random(size);
		for(int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	Double[] NNXX(int size) {
		Double[] random = random(size);
		for(int i = 0; i < size; i += 4) {
			random[i] = null;
			if(i+1 < size)
				random[i+1] = null;
		}
		return random;
	}

	@Override
	Double[] sequential(int size) {
		Double[] elements = new Double[size];
		for (int i = 0; i < size; i++)
			elements[i] = (double)i;
		return elements;
	}

	@Override
	Double[] same(int size) {
		Double[] elements = new Double[size];
		for (int i = 0; i < size; i++)
			elements[i] = 0D;
		return elements;
	}

	@Override
	Double[] smar(int size) {
		Double[] elements = new Double[size];
		double n = 1;
		for (int i = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = n;
		return elements;
	}
	
	@Override
	Double[] notPresent() {
		return new Double[] {-2378457D, -347700D, -88D};
	}
	
	// 1026 values
	private static final Double[] RANDOM = { 5D, 5D, 8D, 5D, 2D, 6D, 9D, 2D, 2D, 8D, 7D, 1D, 5D, 1D, 3D, 4D, 8D, 8D, 3D, 1D, 5D, 6D, 1D, 2D, 1D,
			0D, 1D, 4D, 5D, 9D, 1D, 2D, 2D, 9D, 7D, 8D, 8D, 6D, 2D, 0D, 3D, 9D, 6D, 2D, 6D, 5D, 9D, 8D, 8D, 6D, 3D, 8D, 9D, 3D, 8D, 7D, 2D, 1D, 5D, 9D, 2D,
			2D, 5D, 0D, 6D, 1D, 8D, 8D, 6D, 0D, 1D, 8D, 6D, 2D, 2D, 4D, 1D, 0D, 7D, 4D, 3D, 1D, 3D, 3D, 4D, 0D, 7D, 4D, 7D, 9D, 6D, 9D, 5D, 6D, 6D, 8D, 1D,
			2D, 7D, 2D, 3D, 0D, 4D, 2D, 5D, 6D, 2D, 7D, 1D, 5D, 3D, 3D, 4D, 8D, 8D, 3D, 2D, 5D, 6D, 7D, 6D, 0D, 3D, 7D, 0D, 3D, 9D, 8D, 6D, 1D, 5D, 3D, 6D,
			4D, 3D, 4D, 3D, 2D, 9D, 6D, 3D, 7D, 6D, 3D, 1D, 8D, 7D, 2D, 0D, 1D, 6D, 2D, 4D, 5D, 7D, 4D, 1D, 2D, 4D, 8D, 7D, 2D, 3D, 7D, 4D, 1D, 1D, 1D, 8D,
			9D, 5D, 1D, 2D, 2D, 3D, 0D, 1D, 8D, 9D, 9D, 0D, 2D, 1D, 2D, 3D, 2D, 0D, 4D, 5D, 2D, 9D, 6D, 7D, 6D, 1D, 3D, 0D, 9D, 5D, 5D, 41D, 41D, 14D, 48D,
			23D, 48D, 2D, 93D, 46D, 62D, 70D, 82D, 27D, 80D, 31D, 85D, 64D, 51D, 48D, 37D, 68D, 17D, 49D, 95D, 0D, 2D, 37D, 88D, 61D, 88D, 59D, 90D,
			75D, 23D, 55D, 0D, 15D, 36D, 91D, 96D, 84D, 53D, 81D, 32D, 12D, 15D, 27D, 57D, 85D, 8D, 2D, 13D, 44D, 60D, 53D, 40D, 70D, 75D, 70D, 89D,
			6D, 47D, 35D, 90D, 72D, 53D, 3D, 42D, 0D, 87D, 0D, 35D, 39D, 49D, 40D, 54D, 70D, 44D, 56D, 47D, 72D, 75D, 97D, 93D, 62D, 51D, 19D, 26D,
			71D, 98D, 1D, 36D, 23D, 60D, 94D, 14D, 21D, 97D, 14D, 94D, 84D, 27D, 40D, 87D, 87D, 28D, 12D, 62D, 22D, 37D, 42D, 37D, 73D, 23D, 74D,
			77D, 27D, 29D, 83D, 19D, 60D, 3D, 10D, 27D, 11D, 46D, 88D, 24D, 7D, 62D, 96D, 51D, 74D, 61D, 96D, 6D, 60D, 60D, 96D, 73D, 59D, 33D, 61D,
			29D, 16D, 96D, 75D, 73D, 18D, 79D, 1D, 94D, 15D, 67D, 22D, 82D, 14D, 60D, 69D, 62D, 12D, 61D, 82D, 34D, 72D, 68D, 72D, 18D, 7D, 37D,
			16D, 2D, 50D, 71D, 68D, 26D, 45D, 95D, 82D, 92D, 24D, 66D, 92D, 92D, 57D, 41D, 60D, 62D, 61D, 92D, 0D, 39D, 98D, 12D, 99D, 38D, 31D, 6D,
			8D, 18D, 954D, 598D, 453D, 982D, 918D, 345D, 529D, 8D, 928D, 591D, 726D, 23D, 368D, 744D, 873D, 307D, 890D, 650D, 716D, 175D, 260D,
			772D, 362D, 487D, 52D, 794D, 172D, 516D, 918D, 222D, 700D, 830D, 194D, 663D, 654D, 573D, 764D, 209D, 30D, 395D, 869D, 129D, 972D,
			177D, 407D, 938D, 692D, 637D, 806D, 497D, 658D, 157D, 378D, 625D, 323D, 859D, 161D, 620D, 463D, 668D, 617D, 807D, 651D, 174D,
			219D, 265D, 995D, 366D, 955D, 344D, 348D, 652D, 300D, 792D, 382D, 771D, 913D, 888D, 665D, 809D, 829D, 282D, 311D, 295D, 284D,
			383D, 615D, 821D, 131D, 181D, 530D, 6D, 610D, 191D, 310D, 83D, 362D, 215D, 376D, 950D, 680D, 106D, 798D, 533D, 873D, 808D, 187D,
			945D, 593D, 174D, 59D, 67D, 292D, 474D, 614D, 2D, 877D, 291D, 810D, 979D, 452D, 58D, 848D, 341D, 538D, 656D, 649D, 903D, 201D,
			326D, 770D, 369D, 615D, 763D, 194D, 20D, 617D, 334D, 940D, 503D, 3D, 400D, 943D, 455D, 45D, 748D, 115D, 697D, 834D, 366D, 215D,
			407D, 722D, 486D, 390D, 222D, 943D, 640D, 79D, 272D, 973D, 689D, 435D, 618D, 939D, 967D, 378D, 132D, 374D, 965D, 686D, 328D, 324D,
			450D, 505D, 41D, 501D, 45D, 483D, 609D, 278D, 359D, 307D, 39D, 728D, 749D, 102D, 412D, 485D, 513D, 102D, 480D, 34D, 436D, 466D,
			566D, 824D, 874D, 771D, 513D, 3829D, 4769D, 7270D, 7857D, 7840D, 1671D, 4653D, 2781D, 7553D, 3904D, 4201D, 1686D, 3966D, 2313D,
			8842D, 2798D, 1850D, 3652D, 5214D, 4468D, 9895D, 3152D, 8496D, 7581D, 8893D, 1508D, 5272D, 7009D, 5587D, 8440D, 2449D, 511D,
			1370D, 4719D, 5031D, 8093D, 7946D, 6517D, 143D, 7904D, 2003D, 5058D, 4993D, 8612D, 917D, 2280D, 6304D, 7944D, 7563D, 8318D,
			3834D, 3402D, 1906D, 3337D, 7306D, 7755D, 7944D, 5716D, 4107D, 6814D, 4032D, 1772D, 2119D, 546D, 8354D, 9179D, 5470D, 8793D,
			3834D, 3402D, 1906D, 3337D, 7306D, 7755D, 7944D, 5716D, 4107D, 6814D, 3834D, 3402D, 1906D, 3337D, 7306D, 7755D, 7944D, 5716D,
			4107D, 6814D, 974D, 4815D, 6578D, 6356D, 5713D, 9541D, 3375D, 8398D, 6096D, 1831D, 1976D, 9762D, 6849D, 1760D, 7372D, 2793D,
			6314D, 2824D, 5144D, 9988D, 1905D, 3761D, 1750D, 2242D, 8386D, 7548D, 6339D, 9326D, 2687D, 3384D, 6830D, 5897D, 1489D, 2552D,
			3531D, 831D, 6237D, 8192D, 9992D, 3321D, 2482D, 4124D, 5351D, 4510D, 5535D, 6957D, 1914D, 9638D, 610D, 5713D, 8157D, 4317D,
			170D, 7373D, 6893D, 6593D, 3614D, 3834D, 9383D, 9832D, 4278D, 2202D, 8059D, 7664D, 7639D, 3237D, 8350D, 964D, 3661D, 1236D,
			1855D, 8488D, 1600D, 1465D, 5569D, 962D, 1417D, 6057D, 2055D, 4997D, 4510D, 248D, 5855D, 3323D, 7225D, 751D, 917D, 7454D,
			5319D, 5500D, 8840D, 3427D, 6396D, 7718D, 3337D, 3283D, 8979D, 5418D, 3327D, 9239D, 1571D, 7271D, 6408D, 8752D, 5198D, 4676D,
			8138D, 5665D, 9907D, 9849D, 2744D, 6137D, 2755D, 8928D, 8000D, 1425D, 2048D, 185D, 5361D, 3022D, 4193D, 3860D, 9785D, 867D,
			3891D, 7743D, 6684D, 550D, 6874D, 9836D, 1554D, 2589D, 39285D, 67053D, 64215D, 25493D, 49807D, 95711D, 81334D, 88735D,
			54753D, 80832D, 89535D, 79607D, 76932D, 26335D, 55013D, 94307D, 30501D, 17279D, 75973D, 98388D, 95074D, 51447D, 30752D,
			78140D, 72689D, 39913D, 78729D, 95948D, 59551D, 53437D, 19898D, 50696D, 8213D, 98009D, 53473D, 75914D, 91031D, 1727D,
			34232D, 41616D, 97287D, 64675D, 58962D, 40323D, 72742D, 94855D, 63441D, 78789D, 38298D, 54997D, 44213D, 2550D, 61227D,
			55353D, 8335D, 90850D, 97896D, 66713D, 66259D, 61098D, 80679D, 35857D, 30796D, 17422D, 45833D, 72751D, 99369D, 25044D,
			72603D, 6056D, 19829D, 25240D, 94265D, 74325D, 50149D, 63473D, 33581D, 9070D, 15882D, 71195D, 5205D, 56609D, 77094D, 94309D,
			20675D, 95822D, 22523D, 31545D, 62366D, 57026D, 20287D, 89028D, 49765D, 20878D, 6444D, 23266D, 34740D, 196D, 5396D, 52007D,
			62463D, 51566D, 41681D, 1228D, 19899D, 33450D, 45594D, 81492D, 2268D, 82502D, 40434D, 49561D, 56985D, 42740D, 84603D,
			62680D, 46796D, 48010D, 50920D, 49238D, 30229D, 85913D, 74192D, 54797D, 31614D, 88455D, 11933D, 52024D, 85657D, 85963D,
			98564D, 75351D, 15366D, 29989D, 75632D, 68766D, 62705D, 38071D, 73079D, 93856D, 62744D, 93156D, 23790D, 20820D, 11401D,
			516D, 52632D, 29825D, 91786D, 72513D, 31673D, 12940D, 6291D, 95377D, 79434D, 15130D, 85717D, 15504D, 3493D, 46596D, 54590D,
			39161D, 63871D, 4535D, 97108D, 19040D, 85877D, 71012D, 38303D, 26448D, 53343D, 59243D, 69092D, 82236D, 15618D, 52766D,
			33739D, 68100D, 18082D, 61077D, 3714D, 3654D, 74484D, 17134D, 63098D, 14302D, 22289D, 76991D, 11183D, 81724D, 14726D,
			10746D, 23110D, 32097D, 40776D, 54939D, 60687D, 17397D, 95302D, 68113D, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
			Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY };

}
