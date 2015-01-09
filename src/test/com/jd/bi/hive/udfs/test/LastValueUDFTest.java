package com.jd.bi.hive.udfs.test;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.junit.Test;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

import com.jd.bi.hive.udf.LastValueUDF;

public class LastValueUDFTest {
	
	@Test
	public void testLastValue() {
		LastValueUDF udf = new LastValueUDF();
		DeferredObject[] arguments = new DeferredObject[] {
				new DeferredJavaObject("10"),
				new DeferredJavaObject(30),
				new DeferredJavaObject(12.33),
		};
		try {
			System.out.println(udf.evaluate(arguments));
			arguments = new DeferredObject[] {
					new DeferredJavaObject(null),
					new DeferredJavaObject(30),
					new DeferredJavaObject(12.33),
			};
			System.out.println(udf.evaluate(arguments));
			arguments = new DeferredObject[] {
					new DeferredJavaObject(null),
					new DeferredJavaObject(30),
					new DeferredJavaObject(12.33),
			};
			System.out.println(udf.evaluate(arguments));
			arguments = new DeferredObject[] {
					new DeferredJavaObject(15),
					new DeferredJavaObject(30),
					new DeferredJavaObject(12.33),
			};
			System.out.println(udf.evaluate(arguments));
			arguments = new DeferredObject[] {
					new DeferredJavaObject(null),
					new DeferredJavaObject(30),
					new DeferredJavaObject(12.33),
			};
			System.out.println(udf.evaluate(arguments));
			arguments = new DeferredObject[] {
					new DeferredJavaObject(20),
					new DeferredJavaObject(30),
					new DeferredJavaObject(12.33),
			};
			System.out.println(udf.evaluate(arguments));
		} catch (HiveException e) {
			e.printStackTrace();
		}

	}

}
