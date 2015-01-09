package com.jd.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 查找字符串中的子串，支持正向查找和反向查找
 * @author cuiming
 *
 */
public class IndexStringUDF extends UDF {
	
	public int evaluate(String str,String str_search,int offset_search,int count){
		
		if (offset_search > 0)
			return indexString(str,str_search,offset_search,count);
		else if (offset_search < 0)
			return reverseIndexString(str,str_search,count);
		return -1;
		
	}
	/**
	 * 正向查找
	 * @param string 源字符串
	 * @param str_search 子串
	 * @param offset_search 查找的起始位置，第1位字符为1，第2位字符为2，以此类推
	 * @param count 查找字串的次数
	 * @return 返回查找到的子串的位置 没有找到返回0
	 */
	private int indexString(String string,String str_search,int offset_search,int count){
		int offset = offset_search -1;
		int result = 0;
		for(int i = count; i > 0; i --){
			result = string.indexOf(str_search,offset);
			if (result == -1)
				return 0;
			offset = result + str_search.length();
			if (i == 1)
				return result + 1;
		}
		return 0;
		
	}
	/**
	 * 反向查找
	 * @param string 源字符串
	 * @param str_search 子串
	 * @param count 查找字串的次数
	 * @return 返回查找到的子串的位置 没有找到返回0
	 */
	private int reverseIndexString(String string,String str_search,int count){
		
		int result = 0;
		for(int i = count; i > 0; i --){
			result = string.lastIndexOf(str_search);
			if (result == -1)
				return 0;
			string = string.substring(0,result);
			if (i == 1)
				return result + 1;
		}
		return 0;
		
	}
	public int evaluate(String str,String str_search,int offset){
		return evaluate(str,str_search,offset,1);
	}

	public int evaluate(String str,String str_search){
		return evaluate(str,str_search,1,1);
	}
}
