package com.jd.bi.hive.udf;

import java.math.BigDecimal;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * 求某列的累计和。如果有多个参数，最后一个参数为值，前面的是分组的列；
 * 例如：
 * 月份 金额  累计和
 *  1  100   100
 *  2  200   300
 *  3  350   650
 *  4  500   1150
 *  假设有张雇员薪水表，表的格式 (id int,name string,depno int,salary short/int/long/float/double/string)
 *  其中id为雇员ID，name为雇员名称，depno为部门编号，salary为雇员薪水
 *  使用方法（假设Hive中该函数名为sum_inc）：
 *  1、累计所有雇员薪水和
 *  select *,sum_inc(salary) from 
 *  	(select id,name,depno,salary from emp_salary 
 *  		distribute by depno sort by depno,salary) t;
 *  2、按部门累计薪水和
 *  select *,sum_inc(depno,salary) from 
 *  	(select id,name,depno,salary from emp_salary 
 *  		distribute by depno sort by depno,salary) t;
 * @author cuiming
 *
 */
public class SumIncreaseUDF extends GenericUDF {
	
	private String comparedColumn[] = null;
	
	private PrimitiveObjectInspector inputOI = null;
	
	private ObjectInspector returnInspector = null;
	
	private long sum_long = 0l;
	
	private double sum_double = 0.0d;

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		String columnValue[] = new String[arguments.length -1];
		if (arguments.length > 1)
		{
			for (int i = 1; i < arguments.length; i++)
				columnValue[i-1] = arguments[i].get().toString();
		}
		//第一个数值，初始化comparedColumn数组
		if (comparedColumn == null) {
			comparedColumn = new String[columnValue.length];
			for (int i = 0; i < columnValue.length; i++)
				comparedColumn[i] = columnValue[i];
		}
		//如果当前的各列和之前的列不一致，当前的总和重置为0
		for (int i = 0; i < columnValue.length; i++) {
			if (!comparedColumn[i].equals(columnValue[i])) {
				for (int j = 0; j < columnValue.length; j++) {
					comparedColumn[j] = columnValue[j];
				}
				reset();
				break;
			}
		}
		//将当前值累计了以后返回
		if (returnInspector == PrimitiveObjectInspectorFactory.javaLongObjectInspector){
			long value = 0l;
			try{
				value =PrimitiveObjectInspectorUtils.getLong(arguments[0].get(), inputOI);
			}catch(Exception e){
				value = 0l;
			}
			sum_long += value;
			return sum_long;
		}else if (returnInspector == PrimitiveObjectInspectorFactory.javaDoubleObjectInspector){
			try{
				BigDecimal d1 = new BigDecimal(arguments[0].get().toString());
				BigDecimal d2 = new BigDecimal(sum_double);
				sum_double = d2.add(d1).doubleValue();
			}catch(Exception e){
			}
			return sum_double;
		}
		return null;
	}

	@Override
	public String getDisplayString(String[] arguments) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("sum(");
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
//	    	returnInspector = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
//	    	return returnInspector;
	    case FLOAT:
	    case DOUBLE:
	    case STRING:
	    	returnInspector = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
	    	return returnInspector;
		default:
		      throw new UDFArgumentTypeException(0,
		          "Only numeric or string type arguments are accepted but "
		          + category.name() + " is passed.");
		}
	}
	
	private void reset(){
		sum_long = 0l;
		sum_double = 0.0d;
	}

}
