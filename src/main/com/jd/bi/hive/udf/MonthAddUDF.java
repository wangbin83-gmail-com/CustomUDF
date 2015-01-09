package com.jd.bi.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.jd.bi.hive.util.DateUtil;

public class MonthAddUDF extends UDF {
	
	public String evaluate(String month,int diff) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = DateUtil.monthAdd(month, diff);
		return sdf.format(date);
	}

}
