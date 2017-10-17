package com.sample.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.log4j.Logger;

@Description(name = "ipAddressToIntUDF", value = "returns long value, where x is ip address (STRING)", extended = "SELECT ipAddressToIntUDF('12.12.12.12') from foo limit 1;")
public class AddressInNetCheckerUDF extends UDF {
	private final static Logger log = Logger
			.getLogger(AddressInNetCheckerUDF.class.getName());

	public BooleanWritable evaluate(Text address, Text netAddressMask) {
		try {
			if (address == null || netAddressMask == null)
				return new BooleanWritable(false);
			// net address
			String netstr = netAddressMask.toString();
			int pos = netstr.indexOf("/");
			String addressNetStr = NetMaskAddressLowIntUDF.validateAddress(
					netstr, pos);
			long netAddress = IpAddressToIntUDF.getLong(addressNetStr);
			// address
			String addressStr = address.toString();
			int bitsNumber = NetMaskAddressTopIntUDF.validateMask(netstr, pos);
			long netMask = ~NetMaskAddressTopIntUDF.getNetMaskLong(bitsNumber);
			long addressMasked = (IpAddressToIntUDF.getLong(addressStr) & netMask);

			return new BooleanWritable(netAddress == addressMasked);

		} catch (Exception e) {
			log.error("Error in IpAddressToIntUDF evaluate with param:"
					+ address + " " + netAddressMask, e);
			return new BooleanWritable(false);
		}
	}
}