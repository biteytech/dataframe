package tech.bitey.dataframe;

import java.io.File;
import java.util.Arrays;

public class QuickTest {

	public static void main(String[] args) throws Exception {

		StringColumn column = StringColumn.of(null, "", "foo", "", null);
		DataFrame df = DataFrameFactory.create(new Column<?>[] { column }, new String[] { "COLUMN" });

//		File file = File.createTempFile("emptyString", "csv");
		File file = new File("/home/lior/Desktop/emptyString.txt");
//		file.deleteOnExit();

		df.writeCsvTo(file);

		DataFrame df2 = DataFrameFactory.readCsvFrom(file, new ReadCsvConfig(Arrays.asList(ColumnType.STRING)));
		System.out.println(df2);
	}
}
