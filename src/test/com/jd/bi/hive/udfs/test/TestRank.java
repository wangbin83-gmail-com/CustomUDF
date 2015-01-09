package com.jd.bi.hive.udfs.test;

import org.junit.Test;

import com.jd.bi.hive.udf.RankUDF;

public class TestRank {

	@Test
	public void testFull() {
		System.out.println("**************************testFull***************");
		RankUDF rank = new RankUDF();
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
		System.out.println("**************************testFull***************");
	}
	
	@Test
	public void testOnlyValue() {
		System.out.println();
		System.out.println("**************************testOnlyValue***************");
		RankUDF rank = new RankUDF();
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
