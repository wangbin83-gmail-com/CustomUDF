package com.jd.bi.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import com.jd.bi.hive.util.DateUtil;

/**
 *  补充某个时间段内的数据。例如目前表内数据时 20110101:10 20110120:20，则为了把20110101到20110131这段时间内每天的数据填充。
 * @author cuiming
 *
 */
public class GenericFillPriceUDF extends GenericUDF {
	SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddmmHHss");
	StringObjectInspector soi_starttime = null;
	StringObjectInspector soi_endtime = null;
	MapObjectInspector moi = null;
	IntObjectInspector ioi = null;

	public GenericFillPriceUDF() {
	}

	private void filldata(Date start_date, Date end_date, int step,
			Object last_value, TreeMap<Object, Object> midMap) {
		long diff = DateUtil.diff(start_date, end_date);
		if (diff > 1) {
			for (long i = 0; i < diff / step; i++) {
				Date addDate = DateUtil.addDate(start_date, step * i);
				midMap.put(formatter.format(addDate), last_value);
			}
		}
	}

	@Override
	public Object evaluate(DeferredObject[] objs) throws HiveException {
		Object mapPri = objs[0].get();
		if (mapPri == null)
			return null;
		Map map = moi.getMap(mapPri);

		Object starttimePri = objs[1].get();
		Object endtimePri = objs[2].get();
		if (starttimePri == null || endtimePri == null)
			return translateMap(map);
		String starttime = soi_starttime.getPrimitiveJavaObject(starttimePri);
		String endtime = soi_starttime.getPrimitiveJavaObject(endtimePri);

		Object stepPri = objs[3].get();
		if (stepPri == null)
			return translateMap(map);
		int step = ioi.get(stepPri);

		HashMap<Object, Object> retMap = new HashMap<Object, Object>();
		TreeMap<Object, Object> midMap = new TreeMap<Object, Object>();
		Date start_date = null;
		Date end_date = null;

		try {
			start_date = formatter.parse(starttime.toString());
			System.out.println(start_date.toLocaleString());

			end_date = formatter.parse(endtime.toString());
			System.out.println(end_date.toLocaleString());
		} catch (ParseException e1) {
			System.err.println(e1.toString());
			return translateMap(map);
		}
		Date fill_date = start_date;

		if (end_date.before(start_date)) {
			System.err.println("End time is before start time.");
			return translateMap(map);
		}
		
		try {
			Map<Object, Object> convertMap = new TreeMap<Object, Object>();
			translateMap(map,convertMap);
			// midMap.putAll(map);

			Iterator<Object> keys = convertMap.keySet().iterator();
			Date last_date = null;
			Object last_value = null;
			Date current_date = null;
			while (keys.hasNext()) {
				try {
					Object key = keys.next();
					current_date = formatter.parse(key.toString());
					//1、如果当前时间在结束时间之后，忽略；
					//2、如果当前时间在起始时间之前或者刚好是起始时间，记录，继续循环；
					//3、如果当前时间在其实时间之后，结束时间之前，则进行时间填充；
					if (current_date.after(end_date)) {
						continue;
					} else if (current_date.before(start_date)
							|| start_date.equals(current_date)) {
						last_date = current_date;
						last_value = convertMap.get(key);
						continue;
					} else if (current_date.after(start_date)) {
						if (null == last_date) {
							// System.err.println("Not enough information to fill the data & price.");
							// return map;
							last_date = current_date;
							last_value = convertMap.get(key);
							continue;
						} else {
							if (current_date.before(end_date)) {
								filldata(fill_date, current_date, step,
										last_value, midMap);
								fill_date = current_date;
								last_date = current_date;
								last_value = convertMap.get(key);
								continue;
							} else if (current_date.after(end_date)) {
								filldata(fill_date, end_date, step, last_value,
										midMap);
								fill_date = end_date;
								last_date = current_date;
								last_value = convertMap.get(key);
								break;
							} else {
								filldata(fill_date, end_date, step, last_value,
										midMap);
								fill_date = end_date;
								last_date = current_date;
								last_value = convertMap.get(key);
								break;
							}
						}
					}
				} catch (ParseException e) {
					return translateMap(map);
				}
			}

			//如果提供的数据都在起始时间之前，或者都在结束时间之前，则继续进行补充。
			if (null != last_date) {
				if (last_date.before(end_date)) {
					if (last_date.after(start_date)) {
						filldata(last_date, end_date, step, last_value, midMap);
						fill_date = end_date;
					} else {
						filldata(start_date, end_date, step, last_value, midMap);
						fill_date = end_date;
					}
				}
			}
		} catch (Exception e) {
			return translateMap(map);
		}
		retMap.putAll(midMap);
		
		return retMap;
	}

	@Override
	public String getDisplayString(String[] children) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("{");
	    assert (children.length % 2 == 0);
	    for (int i = 0; i < children.length; i += 2) {
	      sb.append(children[i]);
	      sb.append(":");
	      sb.append(children[i + 1]);
	      if (i + 2 != children.length) {
	        sb.append(",");
	      }
	    }
	    sb.append("}");
	    return sb.toString();
	  }

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length % 4 != 0) {
			throw new UDFArgumentLengthException("参数个数不正确");
		}
		
		moi = (MapObjectInspector) arguments[0];
		soi_starttime = (StringObjectInspector) arguments[1];
		soi_endtime = (StringObjectInspector) arguments[2];
		ioi = (IntObjectInspector) arguments[3];
		return ObjectInspectorFactory.getStandardMapObjectInspector(
				PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				moi.getMapValueObjectInspector());
//		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	private Map translateMap(Map src){
		return translateMap(src,null);
	}
	private Map translateMap(Map src,Map dest){
		if (dest == null)
			dest = new HashMap();
		Iterator entryIter = src.entrySet().iterator();
		Map<Object, Object> convertMap = new TreeMap<Object, Object>();
		while (entryIter.hasNext()) {
			Map.Entry entry = (Map.Entry) entryIter.next();
			dest.put(entry.getKey().toString(), entry.getValue());
		}
		return dest;
	}
}
