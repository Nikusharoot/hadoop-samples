package com.sample.udf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * Not implemented!!!
 * 
 * @author Artem
 *
 */

public class NetAddressExplodeUDTF extends GenericUDTF {

	private PrimitiveObjectInspector stringOI = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		if (argOIs.length != 1) {
			throw new UDFArgumentException(
					"NetAddressExplodeUDTF() takes exactly one argument");
		}

		if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) argOIs[0])
						.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(
					"NetAddressExplodeUDTF() takes a string as a parameter");
		}

		// input inspectors
		stringOI = (PrimitiveObjectInspector) argOIs[0];

		// output inspectors -- an object with two fields!
		List<String> fieldNames = new ArrayList<String>(1);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);
		// fieldNames.add("price");
		fieldNames.add("address_ext");
		// fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);

	}

	@Override
	public void process(Object[] args) throws HiveException {
		final String address = stringOI.getPrimitiveJavaObject(args[0])
				.toString();
		ArrayList<Object[]> results = processInputRecord(address);

		Iterator<Object[]> it = results.iterator();

		while (it.hasNext()) {
			Object[] r = it.next();
			forward(r);
		}

	}

	public ArrayList<Object[]> processInputRecord(String address) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();

		// ignoring null or empty input
		if (address == null || address.isEmpty()) {
			return result;
		}

		for (LongWritable lw : BitsMaskMoveUDF
				.getListOfSubNetAddresses(IpAddressToIntUDF.getLong(address))) {
			result.add(new Object[] { lw.get() });
		}
		return result;
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub

	}

}
