package com.jd.bi.hive.udf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.UDF;

public class URLDecodeUDF extends UDF {
	
	public String evaluate(Object... args) throws UnsupportedEncodingException{
		if (args.length == 0)
			throw new IllegalArgumentException("至少需要1个参数");
		String url_source = args[0].toString();
		String charsetName = "UTF-8";
		if (args.length > 1 && args[1] != null && args[1].toString().trim().length() > 0)
			charsetName = args[1].toString();
		try {
			String url_decoded = URLDecoder.decode(url_source, charsetName);
//			System.out.println("decoded-----" + url_decoded);
			return url_decoded;
		} catch (UnsupportedEncodingException e) {
			return url_source;
		}
	}

}
