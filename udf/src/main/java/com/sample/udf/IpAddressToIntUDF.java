package com.sample.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

;

@Description(name = "ipAddressToIntUDF", value = "returns long value, where x is ip address (STRING)", extended = "SELECT ipAddressToIntUDF('12.12.12.12') from foo limit 1;")
public class IpAddressToIntUDF extends UDF {
	
	public LongWritable evaluate(Text input) {
		if (input == null)
			return null;
		return new LongWritable(getLong(input.toString()));
	}

	protected long getLong(String maskStr) {
		long address = 0;
		for (String part : maskStr.split("\\.")) {
			address = address * 256 + Integer.parseInt(part);
		}
		return address;
	}
}