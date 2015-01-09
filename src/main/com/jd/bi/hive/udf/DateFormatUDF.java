package com.jd.bi.hive.udf;

import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.jd.bi.hive.util.DateUtil;

public class DateFormatUDF extends UDF {
	
	public String evaluate(Object... args) throws ParseException {
		
		if (args.length != 3)
			throw new IllegalArgumentException("²ÎÊý");
		
		Date date = DateUtil.format(args[0].toString(), args[1].toString());
		return DateUtil.format(date, args[2].toString());
	}

}
