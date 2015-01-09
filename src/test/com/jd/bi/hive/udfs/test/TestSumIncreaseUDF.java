package com.jd.bi.hive.udfs.test;

import static org.junit.Assert.*;

import java.math.BigDecimal;

import org.junit.Test;

public class TestSumIncreaseUDF {

	private String[] comparedColumn = null;
	@Test
	public void test1() {
		System.out.println("************************test1*****************");
		tt(new Object[]{21,"10"});
		tt(new Object[]{21,"10"});
		tt(new Object[]{21,"10"});
		tt(new Object[]{21,"20"});
		tt(new Object[]{21,"20"});
		tt(new Object[]{21,"30"});
		System.out.println("************************test1*****************");
	}
	@Test
	public void test2(){
		System.out.println();
		System.out.println("************************test2*****************");
		BigDecimal d1 = new BigDecimal("78.008".toString());
		BigDecimal d2 = new BigDecimal(0.0);
		BigDecimal d3 = new BigDecimal("5677.08");
		d2.add(d1);
		d2.add(d3);
		System.out.println(d2.doubleValue());
		System.out.println("************************test2*****************");
	}
	
	private void tt (Object[] arguments){
		String columnValue[] = new String[arguments.length -1];
		for (int i = 0; i < arguments.length - 1; i++)
			columnValue[i] = arguments[i].toString();
		if (comparedColumn == null) {
			comparedColumn = new String[arguments.length -1];
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
	}
	
	private void reset(){
		System.out.println("reset");
	}
}
