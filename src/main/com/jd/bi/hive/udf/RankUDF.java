package com.jd.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 从查询返回的每一行，计算它们与其它行的相对位置。
 * 根据 DISTRIBUTE BY 子句中表达式的值，从查询返回的每一行，计算它们与其它行的相对位置。
 * 组内的数据按 ORDER BY 子句排序，然后给每一行赋一个号，从而形成一个序列， 该序列从 1 开始，往后累加。
 * 每次 ORDER BY 表达式的值发生变化时，该序列也随之增加。
 * 有同样值的行得到同样的数字序号（认为 null 时相等的）。
 * 然而，如果两行的确得到同样的排序，则序数将随后跳跃。
 * 若两行序数为 1 ，则没有序数 2 ，序列将给组中的下一行分配值 3 ， DENSE_RANK 则没有任何跳跃
 * 
 * 假设有张雇员薪水表，表的格式 (id int,name string,depno int,salary short/int/long/float/double/string)
 * 其中id为雇员ID，name为雇员名称，depno为部门编号，salary为雇员薪水
 * 使用方法（假设Hive中该函数名为rank_over）：
 * a、对所有雇员薪水进行排序
 * select *,rank_over(salary) from 
 * 		(select id,name,depno,salary from emp_salary distribute by depno sort by depno,salary) t;
 * b、按部门分组，对薪水进行排序。
 * select *,rank_over(depno,salary) from 
 * 		(select id,name,depno,salary from emp_salary distribute by depno sort by depno,salary) t;
 * @author cuiming
 *
 */
public class RankUDF extends UDF {
	private String comparedColumn[] = null;
	private String comparedValue = new String();
	//总的行数编号
	private int rowNum = 1;
	//当前行的编号
	private int current_rowNum = 1;

	/**
	 * 参数数组中，最后一个值为比较的值，前面的参数是按order by分组的列
	 * @param args
	 * @return
	 */
	public int evaluate(Object... args) {
		if(args == null || args.length < 1)
			throw new IllegalArgumentException("至少需要1个参数");
		String columnValue[] = new String[args.length -1];
		if (args.length > 1){
			for (int i = 1; i < args.length; i++)
				columnValue[i-1] = args[i].toString();
		}
		String value = args[0].toString();
		//第一行，先复制各列到comparedColumn数组中
		if (comparedColumn == null) {
			comparedColumn = new String[columnValue.length];
			for (int i = 0; i < columnValue.length; i++)
				comparedColumn[i] = columnValue[i];
			comparedValue = value;
			return current_rowNum;
		}
		//如果当前的各列和之前的列不一致，rowNum和current_rowNum重置为1
		for (int i = 0; i < columnValue.length; i++) {
			if (!comparedColumn[i].equals(columnValue[i])) {
				for (int j = 0; j < columnValue.length; j++) {
					comparedColumn[j] = columnValue[j];
				}
				rowNum = 1;
				current_rowNum = 1;
				comparedValue = value;
				return current_rowNum;
			}
		}
		
		//列一致，对值进行比较
		rowNum++;
		if (!comparedValue.equalsIgnoreCase(value))
			current_rowNum = rowNum;
		comparedValue = value;
		return current_rowNum;
	}
}
