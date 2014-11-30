package trial;

import java.io.File;
import java.io.FileInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ReverseRead {
	public static void main(String[] args) throws Exception {
		String fileName = "intro.csv";
		if(args.length > 0)
			fileName = args[0];

		long length = new File(fileName).length();
		FileInputStream fIn = new FileInputStream(fileName);
		MappedByteBuffer in = fIn.getChannel().map(
		    FileChannel.MapMode.READ_ONLY, 0, length);
		long i = length-1;
		while (i >= 0) {
		  System.out.print((char) in.get((int) i));
		  i--;
		}
		
		fIn.close();
	}
}