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

package tech.bitey.bufferstuff.codegen;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

interface GenBufferCode {

	public static void main(String[] args) throws Exception {

		new GenBufferSearch().run();
		new GenBufferSpliterators().run();
		new GenBufferUtils().run();
		new GenBufferSort().run();
		new GenBigByteBuffer().run();
		new GenSmallBuffers().run();
	}

	void run() throws Exception;

	default BufferedWriter open(String srcFileName) throws Exception {
		BufferedWriter out = Files.newBufferedWriter(
				Paths.get(System.getProperty("tech.bitey.bufferstuff.srcDir"), srcFileName), StandardOpenOption.WRITE,
				StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

		section(out, APACHE_LICENSE);

		return out;
	}

	default void section(BufferedWriter out, String text) throws Exception {
		out.write(text);
		out.write('\n');
	}

	static final String APACHE_LICENSE = """
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
			""";
}
