package com.jd.bi.hive.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间工具类
 * @author cuiming
 *
 */
public class DateUtil {

	/**
	 * 相差的小时数
	 * @param d1
	 * @param d2
	 * @return
	 */
	public static long diff(Date d1, Date d2) {
		if (d1 == null || d2 == null)
			return -1;
		Calendar c = Calendar.getInstance();

		c.setTime(d1);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		long l1 = c.getTimeInMillis();

		c.setTime(d2);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		long l2 = c.getTimeInMillis();

		return (l2 - l1) / (1000 * 60 * 60);
	}

	/**
	 * 计算出时间隔后的时间值
	 * @param date 开始时间
	 * @param diff 间隔时间 按小时算，例如计算开始时间2天后的Date，则该参数为48
	 * @return
	 */
	public static Date addDate(Date date, long diff) {
		long time = date.getTime();
		diff = diff * 60 * 60 * 1000;
		time += diff;
		return new Date(time);
	}
	
	public static int getDateOfWeek(String date_str,String format){
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date date = null;
		try {
			date = sdf.parse(date_str);
		} catch (ParseException e) {
			return -1;
		}
		return getDateOfWeek(date);
	}
	
	public static int getDateOfWeek(Date date){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.DAY_OF_WEEK);
	}
	
	public static Date monthAdd(String month_str,int diff) throws ParseException{
		return monthAdd(month_str,diff,"yyyy-MM-dd");
	}
	
	public static Date monthAdd(String month_str,int diff,String format) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date month = sdf.parse(month_str);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(month);
		calendar.add(Calendar.MONDAY, diff);
		return calendar.getTime();
		
	}
	
	public static Date monthFirstDay(String dateStr,String format) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date date = sdf.parse(dateStr);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		return calendar.getTime();
	}
	public static Date format(String dateStr,String format) throws ParseException{
		
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.parse(dateStr);
		
	}
	public static String format(Date date,String format){
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}

}
