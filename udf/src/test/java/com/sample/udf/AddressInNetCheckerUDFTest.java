package com.sample.udf;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class AddressInNetCheckerUDFTest  {

	@Test
	public void testUDF() {
		AddressInNetCheckerUDF example = new AddressInNetCheckerUDF();
		Assert.assertTrue(example.evaluate(new Text("12.21.128.212"),new Text("12.21.128.0/24"))
				.get());
		Assert.assertTrue(example.evaluate(new Text("12.21.127.212"),new Text("12.21.126.0/23"))
				.get());
	}

	@Test
	public void testUDFNullCheck() {
		AddressInNetCheckerUDF example = new AddressInNetCheckerUDF();
		Assert.assertFalse(example.evaluate(null, null).get());
	}
}