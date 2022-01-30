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

package tech.bitey.dataframe.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import tech.bitey.dataframe.Column;
import tech.bitey.dataframe.ShortColumn;
import tech.bitey.dataframe.StringColumn;

public class TestShortColumn extends TestColumn<Short> {

	private static final Random RAND = new Random(0);

	TestShortColumn() {
		super(Short.MIN_VALUE, Short.MAX_VALUE, Short[]::new);
	}

	@Override
	Column<Short> parseColumn(StringColumn stringColumn) {
		return stringColumn.parseShort();
	}

	@Override
	TestSample<Short> wrapSample(String label, Short[] array, int characteristics) {
		ShortColumn column = ShortColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}

	@Override
	TestSample<Short> wrapSample(String label, Short[] array, Column<Short> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override
	Short[] toArray(Collection<Short> samples) {
		return samples.toArray(empty());
	}

	@Override
	Column<Short> collect(Stream<Short> stream) {
		return stream.collect(ShortColumn.collector());
	}

	@Override
	Short[] random(int size) {
		List<Short> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new Short[0]);
	}

	@Override
	Short[] NXNX(int size) {
		Short[] random = random(size);
		for (int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	Short[] NNXX(int size) {
		Short[] random = random(size);
		for (int i = 0; i < size; i += 4) {
			random[i] = null;
			if (i + 1 < size)
				random[i + 1] = null;
		}
		return random;
	}

	@Override
	Short[] sequential(int size) {
		Short[] elements = new Short[size];
		for (int i = 0; i < size; i++)
			elements[i] = (short) i;
		return elements;
	}

	@Override
	Short[] same(int size) {
		Short[] elements = new Short[size];
		for (int i = 0; i < size; i++)
			elements[i] = 0;
		return elements;
	}

	@Override
	Short[] smar(int size) {
		Short[] elements = new Short[size];
		short n = 1;
		for (int i = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = n;
		return elements;
	}

	@Override
	Short[] notPresent() {
		return new Short[] { -23570, -3400, -88 };
	}

	// 1026 values
	private static final Short[] RANDOM = { 5, 5, 8, 5, 2, 6, 9, 2, 2, 8, 7, 1, 5, 1, 3, 4, 8, 8, 3, 1, 5, 6, 1, 2, 1,
			0, 1, 4, 5, 9, 1, 2, 2, 9, 7, 8, 8, 6, 2, 0, 3, 9, 6, 2, 6, 5, 9, 8, 8, 6, 3, 8, 9, 3, 8, 7, 2, 1, 5, 9, 2,
			2, 5, 0, 6, 1, 8, 8, 6, 0, 1, 8, 6, 2, 2, 4, 1, 0, 7, 4, 3, 1, 3, 3, 4, 0, 7, 4, 7, 9, 6, 9, 5, 6, 6, 8, 1,
			2, 7, 2, 3, 0, 4, 2, 5, 6, 2, 7, 1, 5, 3, 3, 4, 8, 8, 3, 2, 5, 6, 7, 6, 0, 3, 7, 0, 3, 9, 8, 6, 1, 5, 3, 6,
			4, 3, 4, 3, 2, 9, 6, 3, 7, 6, 3, 1, 8, 7, 2, 0, 1, 6, 2, 4, 5, 7, 4, 1, 2, 4, 8, 7, 2, 3, 7, 4, 1, 1, 1, 8,
			9, 5, 1, 2, 2, 3, 0, 1, 8, 9, 9, 0, 2, 1, 2, 3, 2, 0, 4, 5, 2, 9, 6, 7, 6, 1, 3, 0, 9, 5, 5, 41, 41, 14, 48,
			23, 48, 2, 93, 46, 62, 70, 82, 27, 80, 31, 85, 64, 51, 48, 37, 68, 17, 49, 95, 0, 2, 37, 88, 61, 88, 59, 90,
			75, 23, 55, 0, 15, 36, 91, 96, 84, 53, 81, 32, 12, 15, 27, 57, 85, 8, 2, 13, 44, 60, 53, 40, 70, 75, 70, 89,
			6, 47, 35, 90, 72, 53, 3, 42, 0, 87, 0, 35, 39, 49, 40, 54, 70, 44, 56, 47, 72, 75, 97, 93, 62, 51, 19, 26,
			71, 98, 1, 36, 23, 60, 94, 14, 21, 97, 14, 94, 84, 27, 40, 87, 87, 28, 12, 62, 22, 37, 42, 37, 73, 23, 74,
			77, 27, 29, 83, 19, 60, 3, 10, 27, 11, 46, 88, 24, 7, 62, 96, 51, 74, 61, 96, 6, 60, 60, 96, 73, 59, 33, 61,
			29, 16, 96, 75, 73, 18, 79, 1, 94, 15, 67, 22, 82, 14, 60, 69, 62, 12, 61, 82, 34, 72, 68, 72, 18, 7, 37,
			16, 2, 50, 71, 68, 26, 45, 95, 82, 92, 24, 66, 92, 92, 57, 41, 60, 62, 61, 92, 0, 39, 98, 12, 99, 38, 31, 6,
			8, 18, 954, 598, 453, 982, 918, 345, 529, 8, 928, 591, 726, 23, 368, 744, 873, 307, 890, 650, 716, 175, 260,
			772, 362, 487, 52, 794, 172, 516, 918, 222, 700, 830, 194, 663, 654, 573, 764, 209, 30, 395, 869, 129, 972,
			177, 407, 938, 692, 637, 806, 497, 658, 157, 378, 625, 323, 859, 161, 620, 463, 668, 617, 807, 651, 174,
			219, 265, 995, 366, 955, 344, 348, 652, 300, 792, 382, 771, 913, 888, 665, 809, 829, 282, 311, 295, 284,
			383, 615, 821, 131, 181, 530, 6, 610, 191, 310, 83, 362, 215, 376, 950, 680, 106, 798, 533, 873, 808, 187,
			945, 593, 174, 59, 67, 292, 474, 614, 2, 877, 291, 810, 979, 452, 58, 848, 341, 538, 656, 649, 903, 201,
			326, 770, 369, 615, 763, 194, 20, 617, 334, 940, 503, 3, 400, 943, 455, 45, 748, 115, 697, 834, 366, 215,
			407, 722, 486, 390, 222, 943, 640, 79, 272, 973, 689, 435, 618, 939, 967, 378, 132, 374, 965, 686, 328, 324,
			450, 505, 41, 501, 45, 483, 609, 278, 359, 307, 39, 728, 749, 102, 412, 485, 513, 102, 480, 34, 436, 466,
			566, 824, 874, 771, 513, 3829, 4769, 7270, 7857, 7840, 1671, 4653, 2781, 7553, 3904, 4201, 1686, 3966, 2313,
			8842, 2798, 1850, 3652, 5214, 4468, 9895, 3152, 8496, 7581, 8893, 1508, 5272, 7009, 5587, 8440, 2449, 511,
			1370, 4719, 5031, 8093, 7946, 6517, 143, 7904, 2003, 5058, 4993, 8612, 917, 2280, 6304, 7944, 7563, 8318,
			3834, 3402, 1906, 3337, 7306, 7755, 7944, 5716, 4107, 6814, 4032, 1772, 2119, 546, 8354, 9179, 5470, 8793,
			3834, 3402, 1906, 3337, 7306, 7755, 7944, 5716, 4107, 6814, 3834, 3402, 1906, 3337, 7306, 7755, 7944, 5716,
			4107, 6814, 974, 4815, 6578, 6356, 5713, 9541, 3375, 8398, 6096, 1831, 1976, 9762, 6849, 1760, 7372, 2793,
			6314, 2824, 5144, 9988, 1905, 3761, 1750, 2242, 8386, 7548, 6339, 9326, 2687, 3384, 6830, 5897, 1489, 2552,
			3531, 831, 6237, 8192, 9992, 3321, 2482, 4124, 5351, 4510, 5535, 6957, 1914, 9638, 610, 5713, 8157, 4317,
			170, 7373, 6893, 6593, 3614, 3834, 9383, 9832, 4278, 2202, 8059, 7664, 7639, 3237, 8350, 964, 3661, 1236,
			1855, 8488, 1600, 1465, 5569, 962, 1417, 6057, 2055, 4997, 4510, 248, 5855, 3323, 7225, 751, 917, 7454,
			5319, 5500, 8840, 3427, 6396, 7718, 3337, 3283, 8979, 5418, 3327, 9239, 1571, 7271, 6408, 8752, 5198, 4676,
			8138, 5665, 9907, 9849, 2744, 6137, 2755, 8928, 8000, 1425, 2048, 185, 5361, 3022, 4193, 3860, 9785, 867,
			3891, 7743, 6684, 550, 6874, 9836, 1554, 2589, 9285, 7053, 4215, 25493, 9807, 5711, 1334, 8735, 4753, 832,
			9535, 9607, 6932, 26335, 5013, 4307, 30501, 17279, 5973, 8388, 5074, 1447, 30752, 8140, 2689, 9913, 8729,
			5948, 9551, 3437, 19898, 696, 8213, 8009, 3473, 5914, 1031, 1727, 4232, 1616, 7287, 4675, 8962, 0323, 2742,
			4855, 3441, 8789, 8298, 5997, 4213, 2550, 1227, 5353, 8335, 850, 7896, 6713, 6259, 1098, 679, 5857, 30796,
			17422, 5833, 2751, 9369, 25044, 2603, 6056, 19829, 25240, 4265, 4325, 149, 3473, 3581, 9070, 15882, 1195,
			5205, 6609, 7094, 4309, 20675, 5822, 22523, 31545, 2366, 7026, 287, 9028, 9765, 20878, 6444, 23266, 4740,
			196, 5396, 2007, 2463, 1566, 1681, 1228, 19899, 3450, 5594, 8149, 2268, 2502, 0434, 9561, 6985, 2740, 4603,
			2680, 6796, 8010, 920, 9238, 30229, 5913, 4192, 5479, 31614, 8455, 11933, 2024, 5657, 5963, 8564, 5351,
			15366, 29989, 5632, 8766, 2705, 8071, 79, 9385, 6274, 3156, 23790, 20820, 11401, 516, 2632, 29825, 1786,
			2513, 31673, 12940, 6291, 377, 9434, 15130, 8571, 15504, 3493, 6596, 4590, 9161, 3871, 4535, 7108, 19040,
			5877, 1012, 8303, 26448, 5334, 5924, 9092, 8223, 15618, 2766, 3739, 8100, 18082, 1077, 3714, 3654, 4484,
			17134, 3098, 14302, 22289, 6991, 11183, 8172, 14726, 10746, 23110, 32097, 776, 4939, 687, 17397, 5302, 8113,
			Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE, Short.MIN_VALUE, Short.MIN_VALUE, Short.MIN_VALUE };
}
