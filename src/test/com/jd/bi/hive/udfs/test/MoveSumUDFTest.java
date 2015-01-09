package com.jd.bi.hive.udfs.test;

import org.junit.Test;

import com.jd.bi.hive.udf.MoveSumUDF;

public class MoveSumUDFTest {

	@Test
	public void testEvaluate() {
		System.out.println("***********************testEvaluate*******************");
		MoveSumUDF udf = new MoveSumUDF();
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println(udf.evaluate(1,3));
		System.out.println("***********************testEvaluate*******************");
	}
	@Test
	public void testEvaluate2() {
		System.out.println("***********************testEvaluate2*******************");
		MoveSumUDF udf = new MoveSumUDF();
		System.out.println(udf.evaluate(1,3,"a"));
		System.out.println(udf.evaluate(1,3,"a"));
		System.out.println(udf.evaluate(1,3,"a"));
		System.out.println(udf.evaluate(1,3,"b"));
		System.out.println(udf.evaluate(1,3,"b"));
		System.out.println(udf.evaluate(1,3,"b"));
		System.out.println(udf.evaluate(1,3,"c"));
		System.out.println(udf.evaluate(1,3,"c"));
		System.out.println(udf.evaluate(1,3,"d"));
		System.out.println(udf.evaluate(1,3,"e"));
		System.out.println(udf.evaluate(1,3,"f"));
		System.out.println("***********************testEvaluate2*******************");
	}

}
