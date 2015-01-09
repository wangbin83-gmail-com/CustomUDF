package com.jd.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 连接数组元素。
 * @author cuiming
 *
 */
public class ConcatArrayUDF extends GenericUDF {

	private ListObjectInspector arrayOI;

	private static final String DEFAULT_DELIMIT = ",";
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object array = arguments[0].get();
	    String delimit = DEFAULT_DELIMIT;
	    if (arguments.length > 1)
	    	delimit = arguments[1].get().toString();
	    StringBuffer sb = new StringBuffer();
	    int arrayLength = arrayOI.getListLength(array);
	    for (int i=0; i<arrayLength; ++i) {
	        String value = arrayOI.getListElement(array, i).toString();
	        sb.append(value);
	        if (i < arrayLength - 1)
	        	sb.append(delimit);
	      }
		return sb.toString();
	}

	@Override
	public String getDisplayString(String[] arguments) {
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length < 1)
			throw new IllegalArgumentException("参数不正确");
		arrayOI = (ListObjectInspector) arguments[0];
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

}
