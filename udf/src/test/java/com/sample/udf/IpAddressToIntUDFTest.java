package com.sample.udf;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class IpAddressToIntUDFTest {

	@Test
	public void testUDF() {
		IpAddressToIntUDF example = new IpAddressToIntUDF();
		Assert.assertEquals((12*256*256*256+21*256*256+128*256+212), example.evaluate(new Text("12.21.128.212"))
				.get());
	}

	@Test
	public void testUDFNullCheck() {
		IpAddressToIntUDF example = new IpAddressToIntUDF();
		Assert.assertNull(example.evaluate(null));
	}
}