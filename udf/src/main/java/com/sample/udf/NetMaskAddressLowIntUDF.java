package com.sample.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

@Description(name = "NetMaskAddressTopIntUDF", 
value = "returns long top value according to net mask, where x is netmask (STRING)", 
extended = "SELECT netMaskAddressTopIntUDF('21.12.12.12/12') from foo limit 1;")
public class NetMaskAddressLowIntUDF extends IpAddressToIntUDF {

	public LongWritable evaluate(Text input) {
		if (input == null)
			return null;
		String str = input.toString();
		int pos = str.indexOf("/");
		String addresskStr = validateAddress(str, pos);
		long ipAddressLong = getLong(addresskStr);

		return new LongWritable(ipAddressLong);
	}

	protected String validateAddress(String input, int pos) {
		String maskStr = input;
		if (pos > 0) {
			maskStr = input.substring(0, pos);
		}
		return maskStr;
	}
}