package com.jd.bi.hive.udfs.test;

import java.util.Date;

import org.junit.Test;

import com.jd.bi.hive.util.DateUtil;

public class TestDateUtil {

	@Test
	public void test() {
		Date date = new Date(System.currentTimeMillis());
		System.out.println(DateUtil.getDateOfWeek(date));
		System.out.println(DateUtil.getDateOfWeek("20120408","yyyyMMdd"));
	}

}
