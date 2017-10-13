package com.sample.udf;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class NetMaskAddressTopIntUDFTest {

	@Test
	public void testUDF() {
		NetMaskAddressTopIntUDF example = new NetMaskAddressTopIntUDF();
		Assert.assertEquals((12*256*256*256+21*256*256+128*256+128+7), 
				example.evaluate(new Text("12.21.128.128/29"))
				.get());
	}

	@Test
	public void testUDFNullCheck() {
		NetMaskAddressTopIntUDF example = new NetMaskAddressTopIntUDF();
		Assert.assertNull(example.evaluate(null));
	}
}