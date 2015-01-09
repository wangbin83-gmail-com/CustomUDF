package com.jd.bi.hive.udf;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.hive.ql.exec.UDF;

public class LagUDF extends UDF {

	private String comparedColumn[] = null;
	
	private Queue<String> queue = new LinkedList<String>();;
	
	private int offset = 0;
	
	private int current_offset = 0;

	public String evaluate(Object... args) {
		if (args == null || args.length < 2)
			throw new IllegalArgumentException("至少需要2个参数");
		
		String columnValue[] = new String[args.length -2];
		if (args.length > 2){
			for (int i = 2; i < args.length; i++)
				columnValue[i-2] = args[i].toString();
		}
		String value = args[0].toString();
		if (comparedColumn == null)
			comparedColumn = Arrays.copyOf(columnValue, columnValue.length);
		
		if (args.length > 1 && offset == 0)
			offset = Integer.parseInt(args[1].toString());
		
		//如果当前的各列和之前的列不一致
		for (int i = 0; i < columnValue.length; i++) {
			if (!comparedColumn[i].equals(columnValue[i])) {
				for (int j = 0; j < columnValue.length; j++) {
					comparedColumn[j] = columnValue[j];
				}
				queue.clear();
				current_offset = 0;
			}
		}
		current_offset++;
		queue.offer(value);
		if (current_offset > offset) 
			return queue.poll();
		return "";
	}
}
