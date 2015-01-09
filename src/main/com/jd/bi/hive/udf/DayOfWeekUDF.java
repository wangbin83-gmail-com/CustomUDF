package com.jd.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.jd.bi.hive.util.DateUtil;

/**
 * 给定日期，获取当前日期是一周中的第几天；周日为第1天，周六为第7天；
 * 参数格式为：第1个参数是日期的字符串数组；第2个参数为日期的格式；
 * 如果没有第2个参数，则默认格式是yyyy-MM-dd
 * @author cuiming
 *
 */
public class DayOfWeekUDF extends UDF {
	
	public int evaluate(Object... args) {
		if(args == null || args.length < 1)
			throw new IllegalArgumentException("至少需要1个参数");
		String date_str = args[0].toString();
		String format = null;
		if (args.length == 2)
			format = args[1].toString();
		if (format == null || format.trim().length() == 0)
			return DateUtil.getDateOfWeek(date_str, "yyyy-MM-dd");
		else
			return DateUtil.getDateOfWeek(date_str,format);
	}

}
