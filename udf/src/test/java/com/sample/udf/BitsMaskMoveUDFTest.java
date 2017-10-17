package com.sample.udf;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BitsMaskMoveUDFTest {

	@Test
	public void testUDF() {
		BitsMaskMoveUDF example = new BitsMaskMoveUDF();
		LongWritable l1 = new LongWritable(12*256*256*256+03*256*256+02*256+02);
		LongWritable l2 = new LongWritable(12*256*256*256+03*256*256+02*256);
		LongWritable l3 = new LongWritable(12*256*256*256+03*256*256);
		LongWritable l4 = new LongWritable(12*256*256*256+02*256*256);
		LongWritable l5 = new LongWritable(12*256*256*256);
		List<LongWritable > obj =  new ArrayList<LongWritable >();
		obj.add(l1);
		obj.add(l2);
		obj.add(l3);
		obj.add(l4);
		obj.add(l5);
		List<LongWritable > result= example.evaluate(new Text("12.03.02.02"));
		assertTrue (result.equals(obj));


		obj =  new ArrayList<LongWritable >();
		obj.add(l5);
		result= example.evaluate(new Text("12.00.00.00"));
		assertTrue (result.equals(obj));

	}

	@Test
	public void testUDFNullCheck() {
		BitsMaskMoveUDF example = new BitsMaskMoveUDF();
		Assert.assertNull(example.evaluate(null));
	}
}