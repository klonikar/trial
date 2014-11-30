package trial;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.DoubleBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

// File format: metadata to describe the schema and then data
// Our simple metadata: 2 integers for number of columns and rows respectively
// then data for each column in column major format
public class WriteReadColumnarBuffers {
	public static final int sizeInt = 4;
	public static final int sizeDouble = 8;
	
	public static void writeFile(String fileName, int numCols, int numRows) throws IOException {
		// length of the file = bytes to store 2 ints for numCols and numRows, and double data numCols*numRows
		int length = 2*sizeInt + numCols*numRows*sizeDouble;
		RandomAccessFile f = new RandomAccessFile(fileName, "rw");
	    MappedByteBuffer out = f.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, length);
	    out.putInt(numCols);
	    out.putInt(numRows);
	    for (int i = 0; i < numCols; i++) {
	    	for(int j = 0;j < numRows;j++) {
	    		out.putDouble((i+1)*(j+1));
	    	}
	    }

	    f.close();
	}

	public static void readFile(String fileName, boolean isNativeByteOrder) throws IOException {
		RandomAccessFile f = new RandomAccessFile(fileName, "rw");
		MappedByteBuffer in = f.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, f.length()).load();
		int numCols = in.getInt(), numRows = in.getInt();
		if(isNativeByteOrder) {
			numCols = Integer.reverseBytes(numCols);
			numRows = Integer.reverseBytes(numRows);
		}
		System.out.println("has array: " + in.hasArray() + ", numCols: " + numCols + ", numRows: " + numRows);
		DoubleBuffer[] columns = new DoubleBuffer[numCols];
		int dataPosition = in.position();
		for(int i = 0;i < numCols;i++) {
			in.limit(dataPosition + (i+1)*numRows*sizeDouble);
			in.position(dataPosition + i*numRows*sizeDouble);
			columns[i] = in.asDoubleBuffer();
		}
		double[] colData = new double[numRows];
	    for (int i = 0; i < numCols; i++) {
	    	 // copy data into colData. Not needed in C/C++. Simple type casting does the job.
	    	columns[i].get(colData);
	    	// print colData
	    	for(int j = 0;j < numRows;j++) {
	    		if(isNativeByteOrder)
	    			System.out.print(Double.longBitsToDouble(Long.reverseBytes(Double.doubleToLongBits(colData[j]))) + ",");
	    		else
	    			System.out.print(colData[j] + ",");
	    	}
	    	System.out.println();
	    }
		
		f.close();
	}
	
	// run with parameters columnarData_c.dat -native to test the native C program generated data
	public static void main(String[] args) throws Exception {
		String fileName = "columnarData.dat";
		int numCols = 3, numRows = 500;
		boolean isNativeMode = false;

		if(args.length > 0)
			fileName = args[0];

		if(args.length > 1 && args[1].equalsIgnoreCase("-native"))
			isNativeMode = true;
		else if(args.length > 1)
			numCols = Integer.parseInt(args[1]);

		if(args.length > 2)
			numRows = Integer.parseInt(args[2]);

		if(!isNativeMode)
			writeFile(fileName, numCols, numRows);
		readFile(fileName, isNativeMode);
	}
}