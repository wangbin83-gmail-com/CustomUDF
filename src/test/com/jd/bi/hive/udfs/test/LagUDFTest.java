package com.jd.bi.hive.udfs.test;

import org.junit.Test;

import com.jd.bi.hive.udf.LagUDF;

public class LagUDFTest {

	@Test
	public void testEvaluate() {
		System.out.println("*********************testEvaluate**************");
		LagUDF udfs = new LagUDF();
		System.out.println(udfs.evaluate("1",2,"a"));
		System.out.println(udfs.evaluate("2",2,"a"));
		System.out.println(udfs.evaluate("3",2,"a"));
		System.out.println(udfs.evaluate("4",2,"a"));
		System.out.println(udfs.evaluate("5",2,"b"));
		System.out.println(udfs.evaluate("6",2,"b"));
		System.out.println(udfs.evaluate("7",2,"c"));
		System.out.println(udfs.evaluate("8",2,"d"));
		System.out.println(udfs.evaluate("9",2,"d"));
		System.out.println(udfs.evaluate("10",2,"d"));
		System.out.println(udfs.evaluate("11",2,"e"));
		
		System.out.println("*********************testEvaluate**************");
	}

}
