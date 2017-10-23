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
import org.apache.log4j.Logger;

/**
 * 
 * @author Artem
 *
 */

public class NetAddressExplodeUDTF extends GenericUDTF {
	private final static Logger log = Logger
			.getLogger(NetAddressExplodeUDTF.class.getName());
	private PrimitiveObjectInspector[] stringOI = new PrimitiveObjectInspector[2];

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		if (argOIs.length != 2) {
			log.error("Error 'argOIs.length != 2' in NetAddressExplodeUDTF initialize with param:"
					+ argOIs);
			throw new UDFArgumentException(
					"NetAddressExplodeUDTF() takes exactly 2 argument");
		}

		if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) argOIs[0])
						.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
			log.error("Error 'with ObjectInspector' in NetAddressExplodeUDTF initialize with param:"
					+ argOIs);
			throw new UDFArgumentException(
					"NetAddressExplodeUDTF() takes a Double as a parameter");
		}
		if (argOIs[1].getCategory() != ObjectInspector.Category.PRIMITIVE
				&& ((PrimitiveObjectInspector) argOIs[1])
						.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			log.error("Error 'with ObjectInspector' in NetAddressExplodeUDTF initialize with param:"
					+ argOIs);
			throw new UDFArgumentException(
					"NetAddressExplodeUDTF() takes a string as a parameter");
		}
		try {
			// input inspectors
			stringOI[0] = (PrimitiveObjectInspector) argOIs[0];
			stringOI[1] = (PrimitiveObjectInspector) argOIs[1];

			// output inspectors -- an object with two fields!
			List<String> fieldNames = new ArrayList<String>(1);
			List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);
			fieldNames.add("price");
			fieldNames.add("address_original");
			fieldNames.add("address_ext");
			fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
			fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
			return ObjectInspectorFactory.getStandardStructObjectInspector(
					fieldNames, fieldOIs);
		} catch (Exception e) {
			log.error("Error  in NetAddressExplodeUDTF initialize with param:"
					+ argOIs, e);
			return null;
		}

	}

	@Override
	public void process(Object[] args) throws HiveException {
		try {
			final Double price = Double.parseDouble(stringOI[0]
					.getPrimitiveJavaObject(args[0]).toString());
			final String address = stringOI[1].getPrimitiveJavaObject(args[1])
					.toString();
			ArrayList<Object[]> results = processInputRecord(price, address);

			Iterator<Object[]> it = results.iterator();

			while (it.hasNext()) {
				Object[] r = it.next();
				forward(r);
			}
		} catch (Exception e) {
			log.error("Error  in NetAddressExplodeUDTF process with param:"
					+ args, e);
		}

	}

	public ArrayList<Object[]> processInputRecord(Double price, String address) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();

		// ignoring null or empty input
		if (address == null || address.isEmpty()) {
			return result;
		}
		long addrLong = IpAddressToIntUDF.getLong(address);
		for (LongWritable lw : BitsMaskMoveUDF
				.getListOfSubNetAddresses(addrLong)) {
			result.add(new Object[] { price, address, lw.get() });
		}
		return result;
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub

	}

}
