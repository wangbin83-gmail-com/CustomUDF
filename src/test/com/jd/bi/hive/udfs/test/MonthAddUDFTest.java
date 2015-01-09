package com.jd.bi.hive.udfs.test;

import java.text.ParseException;

import org.junit.Test;

import com.jd.bi.hive.udf.MonthAddUDF;

public class MonthAddUDFTest {

	@Test
	public void testEvaluate() {
		MonthAddUDF udf = new MonthAddUDF();
		try {
			System.out.println(udf.evaluate("2012-03-05", 2));
			System.out.println(udf.evaluate("2012-03-05", -2));
			System.out.println(udf.evaluate("2012-03-30", -1));
			System.out.println(udf.evaluate("2012-03-05", 12));
			System.out.println(udf.evaluate("2012-03-05", 15));
			System.out.println(udf.evaluate("2012-03-05", -13));
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
