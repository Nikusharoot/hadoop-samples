package com.sample.udf;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NetMaskAddressLowIntUDFTest {

	@Test
	public void testUDF() {
		NetMaskAddressLowIntUDF example = new NetMaskAddressLowIntUDF();
		Assert.assertEquals((12*256*256*256+21*256*256+128*256+212), 
				example.evaluate(new Text("12.21.128.212/31"))
				.get());
	}

	@Test
	public void testUDFNullCheck() {
		NetMaskAddressLowIntUDF example = new NetMaskAddressLowIntUDF();
		Assert.assertNull(example.evaluate(null));
	}
}