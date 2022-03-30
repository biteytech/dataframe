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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.dataframe.BlobColumn;
import tech.bitey.dataframe.StringColumn;

public class TestBlobColumn {

	@Test
	public void basic() {

		StringColumn expected = WANTS.lines().map(l -> l + l + l + l).collect(StringColumn.collector());

		BlobColumn blobs = expected.stream().map(String::getBytes).map(ByteArrayInputStream::new)
				.collect(BlobColumn.collector());

		Assertions.assertEquals(expected, convert(blobs));
		Assertions.assertEquals(expected.subColumn(10, 50), convert(blobs.subColumn(10, 50)));
	}

	private static StringColumn convert(BlobColumn column) {
		return column.toStringColumn(is -> {
			try {
				return new String(is.readAllBytes());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private static final String WANTS = """
					THE WANTS OF MAN.

			"MAN wants but little here below,
			     Nor wants that little long."
			'Tis not with me exactly so;
			     But 'tis so in the song.
			My wants are many and, if told,
			     Would muster many a score;
			And were each wish a mint of gold,
			     I still should long for more.

			What first I want is daily bread –
			     And canvas-backs – and wine –
			And all the realms of nature spread
			     Before me, where I dine.
			Four courses scarcely can provide
			     My appetite to quell;
			With four choice cooks from France beside,
			     To dress my dinner well.

			What next I want, at princely cost,
			     Is elegant attire:
			Black sable furs for winter's frost,
			     And silk for summer's fire,
			And Cashmere shawls, and Brussel's lace
			     My bosom's front to deck, –
			And diamond rings my hands to grace,
			     And rubies for my neck.

			I want (who does not want?) a wife, –
			     Affectionate and fair;
			To solace all the woes of life,
			     And all its joys to share.
			Of temper sweet, of yielding will,
			     Of firm, yet placid mind, –
			With all my faults to love me still
			     With sentiment refined.

			And as Time's car incessant runs,
			     And Fortune fills my store,
			I want of daughters and of sons
			     From eight to half a score.
			I want (alas! can mortal dare
			     Such bliss on earth to crave?)
			That all the girls be chaste and fair, –
			     The boys all wise and brave.

			I want a warm and faithful friend,
			     To cheer the adverse hour;
			Who ne'er to flatter will descend,
			     Nor bend the knee to power, –
			A friend to chide me when I'm wrong,
			     My inmost soul to see;
			And that my friendship prove as strong
			     To him as his to me.

			I want the seals of power and place,
			     The ensigns of command;
			Charged by the People's unbought grace
			     To rule my native land.
			Nor crown nor sceptre would I ask,
			     But from my country's will,
			By day, by night, to ply the task
			     Her cup of bliss to fill.

			I want the voice of honest praise
			     To follow me behind,
			And to be thought in future days
			     The friend of human-kind,
			That after ages, as they rise,
			     Exulting may proclaim
			In choral union to the skies
			     Their blessings on my name.

			These are the Wants of mortal Man, –
			     I cannot want them long,
			For life itself is but a span,
			     And earthly bliss – a song.
			My last great Want – absorbing all –
			     Is, when beneath the sod,
			And summoned to my final call,
			     The Mercy of my God.

				– JOHN QUINCY ADAMS
			""";
}
