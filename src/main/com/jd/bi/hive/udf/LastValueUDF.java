package com.jd.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class LastValueUDF extends GenericUDF {

	private PrimitiveObjectInspector inputOI = null;
	private ObjectInspector returnInspector = null;
	private Object last_value = null;
	private String comparedColumn[] = null;
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		String columnValue[] = new String[arguments.length -1];
		if (arguments.length > 1)
		{
			for (int i = 1; i < arguments.length; i++)
				columnValue[i-1] = arguments[i].get().toString();
		}
		if (comparedColumn == null) {
			comparedColumn = new String[columnValue.length];
			for (int i = 0; i < columnValue.length; i++)
				comparedColumn[i] = columnValue[i];
		}
		for (int i = 0; i < columnValue.length; i++) {
			if (!comparedColumn[i].equals(columnValue[i])) {
				for (int j = 0; j < columnValue.length; j++) {
					comparedColumn[j] = columnValue[j];
				}
				reset();
				break;
			}
		}
		boolean is_null = false;
		Object value = 0.0d;
		try{
			value = arguments[0].get();
		}catch(Exception e){
			is_null = true;
		}
		if (value == null || is_null) {
			return last_value;
		} else {
			last_value = value;
			return value;
		}
	}

	private void reset(){
		last_value = null;
	}
	@Override
	public String getDisplayString(String[] arguments) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("last_value(");
	    for (int i = 0; i < arguments.length; i ++) {
	      sb.append(arguments[i]);
	      if (i != arguments.length - 1) {
	        sb.append(",");
	      }
	    }
	    sb.append(")");
	    return sb.toString();
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		reset();
		if (arguments.length < 1)
			throw new UDFArgumentException("至少需要一个参数");
		inputOI = (PrimitiveObjectInspector)arguments[0];
		PrimitiveCategory category = inputOI.getPrimitiveCategory();
		switch(category){
			case BYTE:
		    case SHORT:
		    case INT:
		    case LONG:
		    case FLOAT:
		    case DOUBLE:
		    case STRING:
		    	returnInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		    	return returnInspector;
			default:
			      throw new UDFArgumentTypeException(0,
			          "Only numeric or string type arguments are accepted but "
			          + category.name() + " is passed.");
		}
	}
}
