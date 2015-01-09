package com.jd.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 获取当前行的编号。
 * 编号规则：
 * a、如果只有一个参数
 * 比较当前值和前一个值，如果值不同，则当前行编号是前一行编号加1；否则编号不变；
 * b、如果有多个参数
 * 最后一个参数是比较的值，前面的参数分组的列；
 * 先比较分组的列，如果分组的列不同，则编号重置为1
 * 如果比较的列相同，则按(a)中的规则进行比较。
 * @author cuiming
 *
 */
public class RowNumberUDF extends UDF {
	private static int MAX_VALUE = 50;
	private String comparedColumn[] = new String[MAX_VALUE];
	private int rowNum = 1;

	public int evaluate(Object... args) {
		if (args == null || args.length == 0) {
			return rowNum++;
		} else {
			String columnValue[] = new String[args.length];
			for (int i = 0; i < args.length; i++)
				columnValue[i] = args[i].toString();
			if (rowNum == 1) {
				for (int i = 0; i < columnValue.length; i++)
					comparedColumn[i] = columnValue[i];
			}
			for (int i = 0; i < columnValue.length; i++) {
				if (!comparedColumn[i].equals(columnValue[i])) {
					for (int j = 0; j < columnValue.length; j++) {
						comparedColumn[j] = columnValue[j];
					}
					rowNum = 1;
					return rowNum++;
				}
			}
			return rowNum++;
		}
	}
}
