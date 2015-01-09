package com.jd.bi.hive.udfs.test;

import java.text.ParseException;

import org.junit.Test;

import com.jd.bi.hive.udf.MonthFirstrDayUDF;

public class MonthFirstrDayUDFTest {

	@Test
	public void testEvaluate() throws ParseException {
		MonthFirstrDayUDF udf = new MonthFirstrDayUDF();
		System.out.println(udf.evaluate("2012-02-23"));
		System.out.println(udf.evaluate("2012-03-23"));
		System.out.println(udf.evaluate("2012-02-28"));
		System.out.println(udf.evaluate("2012-12-31"));
		System.out.println(udf.evaluate("2012-01-01"));
	}

}
