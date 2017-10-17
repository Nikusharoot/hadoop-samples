package com.sample.udf;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class BitsMaskMoveUDF extends UDF {
	private final static Logger log = Logger.getLogger(BitsMaskMoveUDF.class
			.getName());

	public List<LongWritable> evaluate(Text input) {
		try {
			if (input == null)
				return null;
			String str = input.toString();
			int pos = str.indexOf("/");

			String addressStr = NetMaskAddressLowIntUDF.validateAddress(str,
					pos);
			long address = IpAddressToIntUDF.getLong(addressStr);
			return getListOfSubNetAddresses(address);
		} catch (Exception e) {
			log.error("Error in BitsMaskMoveUDF evaluate with param:" + input,
					e);
			return null;
		}
	}

	static public List<LongWritable> getListOfSubNetAddresses(long address) {
		List<LongWritable> result = new ArrayList<LongWritable>(24);
		long mask = ~0;
		int shiftsCount = 1;
		long previouse = -1;
		while (shiftsCount < 25) {
			long changed = address & mask;
			if (changed != previouse) {
				result.add(new LongWritable(changed));
			}
			previouse = changed;
			mask = mask << 1;
			shiftsCount++;
		}

		return result;
	}
}
