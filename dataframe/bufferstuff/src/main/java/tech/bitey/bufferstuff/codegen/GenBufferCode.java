package tech.bitey.bufferstuff.codegen;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

interface GenBufferCode {

	public static void main(String[] args) throws Exception {

		new GenBufferSearch().run();
		new GenBufferSpliterators().run();
		new GenBufferUtils().run();
		new GenBufferSort().run();
	}

	void run() throws Exception;

	default BufferedWriter open(String srcFileName) throws IOException {
		return Files.newBufferedWriter(Paths.get(System.getProperty("tech.bitey.bufferstuff.srcDir"), srcFileName),
				StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
	}

	default void section(BufferedWriter out, String text) throws Exception {
		out.write(text);
		out.write('\n');
	}
}
