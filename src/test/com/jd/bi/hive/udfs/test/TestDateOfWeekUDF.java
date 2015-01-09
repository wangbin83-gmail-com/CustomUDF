package com.jd.bi.hive.udfs.test;

import org.junit.Test;

import com.jd.bi.hive.udf.DayOfWeekUDF;

public class TestDateOfWeekUDF {

	@Test
	public void testEvaluate() {
		DayOfWeekUDF udf = new DayOfWeekUDF();
		System.out.println(udf.evaluate("2012-04-07"));
		System.out.println(udf.evaluate("2012-04-08","yyyy-MM-dd"));
		System.out.println(udf.evaluate("2012-04-01","yyyy-MM-dd"));
		System.out.println(udf.evaluate("20120308","yyyyMMdd"));
	}

}
