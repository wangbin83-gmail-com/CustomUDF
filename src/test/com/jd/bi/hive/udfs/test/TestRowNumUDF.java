package com.jd.bi.hive.udfs.test;

import org.junit.Test;

import com.jd.bi.hive.udf.RowNumberUDF;

public class TestRowNumUDF {

	@Test
	public void testWithParam() {
		System.out.println("**************************testWithParam***************");
		RowNumberUDF t = new RowNumberUDF();
		System.out.println(t.evaluate(123,12));
		System.out.println(t.evaluate(123,12));
		System.out.println(t.evaluate(123,10));
		System.out.println(t.evaluate(123,20));
		System.out.println(t.evaluate(1234,11));
		System.out.println(t.evaluate(1234,11));
		System.out.println(t.evaluate(1234,21));
		System.out.println(t.evaluate(1234,30));
		System.out.println(t.evaluate(1235,11));
		System.out.println("**************************testWithParam***************");
	}
	
	@Test
	public void testNoParam() {
		System.out.println();
		System.out.println("**************************testNoParam***************");
		RowNumberUDF t = new RowNumberUDF();
		System.out.println(t.evaluate());
		System.out.println(t.evaluate());
		System.out.println(t.evaluate());
		System.out.println(t.evaluate());
		System.out.println(t.evaluate());
		System.out.println(t.evaluate());
		System.out.println(t.evaluate());
		System.out.println("**************************testNoParam***************");
	}

}
