package com.sample.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

@Description(name = "ipAddressToIntUDF", value = "returns long value, where x is ip address (STRING)", extended = "SELECT ipAddressToIntUDF('12.12.12.12') from foo limit 1;")
public class IpAddressToIntUDF extends UDF {
	private final static Logger log = Logger.getLogger(IpAddressToIntUDF.class
			.getName());

	public LongWritable evaluate(Text input) {
		try {
			if (input == null)
				return null;
			return new LongWritable(getLong(input.toString()));
		} catch (Exception e) {
			log.error(
					"Error in IpAddressToIntUDF evaluate with param:" + input,
					e);
			return null;
		}
	}

	static protected long getLong(String maskStr) {
		if (maskStr.length() < 7) {
			return -1;
		}
		long address = 0;
		String[] arr = maskStr.split("\\.");
		if (arr.length != 4) {
			return -1;
		}
		for (String part : arr) {
			int intPart = 0;
			try {
				intPart = Integer.parseInt(part);
			} catch (Exception e) {
				return -1;
			}
			address = address * 256 + intPart;
		}
		return address;
	}
}