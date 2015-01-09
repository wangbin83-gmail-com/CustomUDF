package com.jd.bi.hive.udf;

import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.jd.bi.hive.util.DateUtil;

public class MonthFirstrDayUDF extends UDF {
	
	public String evaluate(String dateStr) throws ParseException{
		Date date = DateUtil.monthFirstDay(dateStr, "yyyy-MM-dd");
		return DateUtil.format(date, "yyyy-MM-dd");
	}

}
