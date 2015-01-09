package com.jd.bi.hive.udfs.test;

import static org.junit.Assert.fail;

import org.junit.Test;

import com.jd.bi.hive.udf.IndexStringUDF;

public class StringSearchUDFTest {

	@Test
	public void testEvaluateStringStringIntInt() {
		System.out.println("******************testEvaluateStringStringIntInt************");
		IndexStringUDF udfs = new IndexStringUDF();
		System.out.println(udfs.evaluate("abcdedfsdadfsfaadafdfsdf", "d", 5, 1));
		System.out.println(udfs.evaluate("abcdedfsdadfsfaadafdfsdf", "d", 1, 2));
		System.out.println(udfs.evaluate("abcdedfsdadfsfaadafdfsdf", "d", 1, 5));
		System.out.println(udfs.evaluate("abcdedfsdadfsfaadafdfsdf", "df", -1, 3));
		System.out.println(udfs.evaluate("abcdedfsdadfsfaadafdfsdf", "d", 1, 10));
		System.out.println("******************testEvaluateStringStringIntInt************");
	}

	@Test
	public void testEvaluateStringStringInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testEvaluateStringString() {
		fail("Not yet implemented");
	}

}
