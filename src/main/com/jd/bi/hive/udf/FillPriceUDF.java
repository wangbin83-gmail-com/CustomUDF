package com.jd.bi.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.jd.bi.hive.util.DateUtil;

public class FillPriceUDF extends UDF{
	SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddmmHHss");

	public FillPriceUDF()
	{

	}
	
	private void  filldata( Date start_date, Date end_date, int step, Double last_value, TreeMap<String, Double> midMap)
	{
		long diff = DateUtil.diff(start_date, end_date);
		if (diff > 1) {
			for (long i = 0; i < diff / step; i++) {
				Date addDate = DateUtil.addDate(start_date, step*i);
				midMap.put(formatter.format(addDate), last_value);
			}
		} 
	}
	

	public HashMap<String, Double> evaluate(HashMap<String, Double> map, String startTime,
			String endTime, int step) {
		HashMap<String, Double> retMap = new HashMap<String, Double>();
		TreeMap<String, Double> midMap = new TreeMap<String, Double>();
		int i = 0;
		Date start_date = null;
		Date end_date = null;
		
		try {
			start_date = formatter.parse(startTime.toString());
			System.out.println(start_date.toLocaleString());
			
			end_date = formatter.parse(endTime.toString());
			System.out.println(end_date.toLocaleString());
		} catch (ParseException e1) {
			System.err.println(e1.toString());
			return map;
		}
		Date fill_date = start_date;
		
		if(end_date.before(start_date)){
			System.err.println("End time is before start time.");
			return map;
		}
		
		Map<String, Double> convertMap = new TreeMap<String, Double>();
		convertMap.putAll(map);
//		midMap.putAll(map);
		
		
		Iterator<String> keys = convertMap.keySet().iterator();
		Date last_date = null;
		Double last_value = null;
		Date current_date = null;
		long diff;
		while (keys.hasNext()) {
			try {
				String key =  keys.next();

				current_date = formatter.parse(key.toString());
				if (current_date.after(end_date)) {
					System.err.println("62:\tCUR DATE\t" + current_date.toLocaleString());
					continue;
				} else	if (current_date.before(start_date) || start_date.equals(current_date)){
					last_date = current_date;
					last_value = map.get(key);
					continue;
				} else if (current_date.after(start_date)) {
					if(null == last_date) {
//						System.err.println("Not enough information to fill the data & price.");
//						return map;
						last_date = current_date;
						last_value = map.get(key);
						continue;
					} else {
						if(current_date.before(end_date)) {
							filldata(fill_date, current_date, step, last_value, midMap);
							fill_date = current_date;
							last_date = current_date;
							last_value = map.get(key);
							continue;
						} else if(current_date.after(end_date)) {
							filldata(fill_date, end_date, step, last_value, midMap);
							fill_date = end_date;
							last_date = current_date;
							last_value = map.get(key);
							break;
						} else {
							filldata(fill_date, end_date, step, last_value, midMap);
							fill_date = end_date;
							last_date = current_date;
							last_value = map.get(key);
							break;
						}
					}
				}
			} catch (ParseException e) {
			}
		}
		
		if(null !=  last_date) {
			if(last_date.before(end_date)) {
				if(last_date.after(start_date)) {
					filldata(last_date, end_date, step, last_value, midMap);
					fill_date= end_date;
				} else {
					filldata(start_date, end_date, step, last_value, midMap);
					fill_date = end_date;
				}
			}
		}
		
		retMap.putAll(midMap);
		return retMap;

	}
	public static void main(String[] args){
		HashMap<String, Double> map = new HashMap<String, Double>();
		
//		map.put("20110818000000", 1.0);
//		map.put("20120203000000", 2.0);
//		map.put("20120207000000", 3.0);
//		map.put("20120210000000", 3.5);
//		map.put("20120224000000", 4.0);
//		map.put("20101024000000", 5.0);
//		map.put("20091024000000", 6.0);
		System.out.println("\n\n###############test case 1 ##################");
		map.clear();
		map.put("20110831000000", 17.0);
		map.put("20111009000000", 44.9);
		map.put("20111009000000", 17.0);
		FillPriceUDF udf = new FillPriceUDF();
		Map retmap = udf.evaluate(map, new String("20120201000000"),new String("20120301000000"), 24);
		TreeMap finalMap = new TreeMap();
		finalMap.putAll(retmap);
		Iterator iterator = finalMap.keySet().iterator();
		System.out.println("\n\n");
		while(iterator.hasNext()){
			Object key = iterator.next();
			System.out.println(key + ":" + finalMap.get(key));
		}
		
		System.out.println("\n\n###############test case 2 ##################");
		map.clear();
		map.put("20120301000000", 18.0);
		retmap = udf.evaluate(map, new String("20120201000000"),new String("20120328000000"), 24);
		finalMap.clear();
		finalMap.putAll(retmap);
		iterator = finalMap.keySet().iterator();
		System.out.println("\n\n");
		while(iterator.hasNext()){
			Object key = iterator.next();
			System.out.println(key + ":" + finalMap.get(key));
		}
		
		System.out.println("\n\n###############test case 3 ##################");
		map.clear();
		map.put("20110831000000", 17.0);
		map.put("20111009000000", 44.9);
		map.put("20111009000000", 17.0);
		map.put("20120209000000", 21.0);
		retmap = udf.evaluate(map, new String("20120201000000"),
				new String("20120301000000"), 24);
		finalMap = new TreeMap();
		finalMap.putAll(retmap);
		iterator = finalMap.keySet().iterator();
//		System.out.println("\n\n");
		while (iterator.hasNext()) {
			Object key = iterator.next();
			System.out.println(key + ":" + finalMap.get(key));
		}

		System.out.println("\n\n###############test case 4 ##################");
		map.clear();
		map.put("20110831000000", 17.0);
		map.put("20111009000000", 44.9);
		map.put("20111009000000", 17.0);
		map.put("20120201000000", 20.5);
		map.put("20120209000000", 21.0);
		map.put("20120219000000", 20.0);
		map.put("20120229000000", 29.0);
		map.put("20120301000000", 31.0);
		retmap = udf.evaluate(map, new String("20120201000000"),
				new String("20120328000000"), 24);
		finalMap = new TreeMap();
		finalMap.clear();
		finalMap.putAll(retmap);
		iterator = finalMap.keySet().iterator();
		System.out.println("\n\n");
		while (iterator.hasNext()) {
			Object key = iterator.next();
			System.out.println(key + ":" + finalMap.get(key));
		}

		System.out.println("\n\n###############test case 5 ##################");
		map.clear();
		map.put("20120301000000", 18.0);
		retmap = udf.evaluate(map, new String("20120201000000"), new String(
				"20120328000000"), 24);
		finalMap.clear();
		finalMap.putAll(retmap);
		iterator = finalMap.keySet().iterator();
		System.out.println("\n\n");
		while (iterator.hasNext()) {
			Object key = iterator.next();
			System.out.println(key + ":" + finalMap.get(key));
		}
	}
}
