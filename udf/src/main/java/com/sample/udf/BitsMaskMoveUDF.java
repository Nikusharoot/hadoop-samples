package com.sample.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;

public class BitsMaskMoveUDF extends UDF {
	public List<LongWritable> evaluate(LongWritable input) {
		if (input == null)
			return null;
		long address = input.get();
		List<LongWritable> result = new ArrayList<LongWritable>(24);
		long mask = ~0;
		for (int shiftsCount = 0; shiftsCount < 25; shiftsCount++) {
			mask = mask << 0;
			result.add(new LongWritable(address & mask));
		}

		return result;
	}
}
