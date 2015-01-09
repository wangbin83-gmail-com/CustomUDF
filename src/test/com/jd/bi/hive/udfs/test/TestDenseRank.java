package com.jd.bi.hive.udfs.test;

import org.junit.Test;

import com.jd.bi.hive.udf.DenseRankUDF;

public class TestDenseRank {

	@Test
	public void testDigit() {
		System.out.println("**************************testDigit***************");
		DenseRankUDF rank = new DenseRankUDF();
		System.out.println(rank.evaluate(2000,"10"));
		System.out.println(rank.evaluate(2000,"10"));
		System.out.println(rank.evaluate(2100,"10"));
		System.out.println(rank.evaluate(2200,"10"));
		System.out.println(rank.evaluate(2000,"20"));
		System.out.println(rank.evaluate(2100,"20"));
		System.out.println(rank.evaluate(2100,"20"));
		System.out.println(rank.evaluate(2200,"20"));
		System.out.println(rank.evaluate(2050,"30"));
		System.out.println(rank.evaluate(2100,"30"));
		System.out.println("**************************testDigit***************");
	}
	@Test
	public void testNull() {
		System.out.println();
		System.out.println("**************************testNull***************");
		DenseRankUDF rank = new DenseRankUDF();
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println(rank.evaluate("null","null"));
		System.out.println("**************************testNull***************");
	}
	@Test
	public void testOnlyValue() {
		System.out.println();
		System.out.println("**************************testOnlyValue***************");
		DenseRankUDF rank = new DenseRankUDF();
		System.out.println(rank.evaluate(2000));
		System.out.println(rank.evaluate(2000));
		System.out.println(rank.evaluate(2100));
		System.out.println(rank.evaluate(2200));
		System.out.println(rank.evaluate(2000));
		System.out.println(rank.evaluate(2100));
		System.out.println(rank.evaluate(2100));
		System.out.println(rank.evaluate(2200));
		System.out.println(rank.evaluate(2050));
		System.out.println(rank.evaluate(2100));
		System.out.println("**************************testOnlyValue***************");
	}
}
