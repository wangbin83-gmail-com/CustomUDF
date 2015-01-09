package com.jd.bi.hive.udfs.test;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

import com.jd.bi.hive.udf.URLDecodeUDF;

public class URLDecodeUDFTest {

	@Test
	public void testEvaluate() throws UnsupportedEncodingException {
		URLDecodeUDF udf = new URLDecodeUDF();
		System.out.println(udf.evaluate("%bb%d8%c1%a640","GBK"));
//		System.out.println(URLDecoder.decode(udf.evaluate("http://www.baidu.com/s?tn=baiduhome_pg&bs=%D7%D6%B7%FB%BC%AF&f=8&rsv_bp=1&rsv_spt=1&wd=java+%C8%D5%C6%DA+%D4%C2+%BC%D3&inputT=5619","GBK"),"UTF-8"));
	}

}
