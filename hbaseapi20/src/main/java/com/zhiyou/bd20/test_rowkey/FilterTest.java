package com.zhiyou.bd20.test_rowkey;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.zhiyou.bd20.hbasetest.HBaseUtil;

public class FilterTest {
	// 查询创建时间等于20120904的所有数据

	// 使用rowkey的filter来进行比较,需要判断rowkey的数据内部要包含20120904
	// 操作符 equal
	private Table table;
	private SimpleDateFormat format =new SimpleDateFormat("yyyyMMdd");
	public FilterTest() {
		table = HBaseUtil.getTable("bd20:file_info");
	}

	public void searchByCreateTime(String createTime) throws Exception{
		Scan scan = new Scan();
		long cTime = format.parse(createTime).getTime();
		byte[] ltobytes = Bytes.toBytes(cTime);
		//
		SubstringComparator comparator = new SubstringComparator(Bytes.toString(ltobytes));
		//作比较
		Filter filter = new RowFilter(CompareOp.EQUAL,comparator);
		
		scan.setFilter(filter);
		
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}
	
	
	//
	public void searchBy(String userId,String createTime) throws Exception{
		//使用rowfilter查询出userid=3 并且 createtime =20120918的数据内容
		//rowkey =userId+create time
		//过滤器RowFilter
		//比较器BinaryPrefixComparator
		//运算符:compareOp.equal
		ByteBuffer  prefix=ByteBuffer.allocate(14);
		ByteBuffer userbuffer = ByteBuffer.allocate(6);
		userbuffer.put(Bytes.toBytes(StringUtils.reverse(userId)));
		//不足补零
		while (userbuffer.hasRemaining()) {
			userbuffer.put(Bytes.toBytes("0"));
		}
		long cTime = format.parse(createTime).getTime();
		prefix.put(userbuffer).array();
		prefix.put(Bytes.toBytes(cTime));
		//创建比较器
		BinaryPrefixComparator comparator = new BinaryPrefixComparator(prefix.array());
		//创建过滤器
		Filter filter = new RowFilter(CompareOp.EQUAL,comparator);
		//把过滤器设置到scan
		Scan scan = new Scan();
		scan.setFilter(filter);
		//调用table的scan
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}
	
	//比较器regexStringComparator(SubstringComparator,BinaryPrefixComparator)
	//过滤器:qualifyFilter
	public void searchByRex() throws Exception{
		//定义过滤器
		RegexStringComparator comparator 
			=new RegexStringComparator("^phone_\\d+");
		//定义比较器
		Filter filter = new QualifierFilter(CompareOp.EQUAL,comparator);
		//定义scan,scan setfilter
		Scan scan = new Scan();
		scan.setFilter(filter);
		//定义table.getscanner
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}
	
	
	//查询列值中包含"中国好声音"的数据
	//过滤器:ValueFilter
	//比较器:SubstringComparator
	//操作符:CompareOp.EQUAL
	public void searchByFileName(String value) throws Exception{
		//BinaryPrefixComparator comparator = new BinaryComparator(Bytes.toBytes(value));
		SubstringComparator comparator = new SubstringComparator(value);
		Filter filter = new ValueFilter(CompareOp.EQUAL,comparator);
		//定义scan,scan setfilter
		Scan scan = new Scan();
		scan.setFilter(filter);
		//定义table.getscanner
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}
	//查询文件中包含"中国好声音"的数据
	//1:列簇为i
	//2:列名称name
	//3:列值;包含中国好声音的
	//该需求其实需要三个filter来完成对扫描的限定
	//3个filter是and关系
	//使用filterList 结构过滤器
	public void getDataByFileNameRight(String value) throws Exception{
		//定义第一个过滤器 限定列簇为i 使用familyFilter   BinaryComparator CompareOp.EQUAL
		//定义第二个:限定列名称name  QualifierFilter   BinaryComparator CompareOp.EQUAL
		//定义第三个  valuefilter SubstringComparator CompareOp.EQUAL
		//定义FilterList过滤器,把前三个过滤器加到FilterList中
		
		//把filter添加到scan上
		//用table来配置scan对数据扫描,并获取到数据结果
		
		//1
		BinaryComparator bComparator = new BinaryComparator(Bytes.toBytes("i"));
		FamilyFilter filter1= new FamilyFilter(CompareOp.EQUAL,bComparator);
		//2
		BinaryComparator bComparator2 = new BinaryComparator(Bytes.toBytes("name"));
		Filter filter2 = new ValueFilter(CompareOp.EQUAL,bComparator2);
		
		//3
		SubstringComparator comparator = new SubstringComparator(value);
		FamilyFilter filter3 = new FamilyFilter(CompareOp.EQUAL,comparator);
		//将三个filter添加到 FilterList
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		filterList.addFilter(filter3);
		
		//定义scan  scan.setfilter
		Scan scan = new Scan();
		scan.setFilter(filterList);
		//定义table table.getscanner
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}
	
	//专有过滤器
	public void getPhoneFromPerson() throws Exception{
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("userId"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table .getScanner(scan);
		HBaseUtil.printResultScanner(scanner);
	}
	
	
	//fuzzyFilter
	//查询"20120910"
	//查询"20120910-20120914"
	public void fuzzyFilterTest() throws Exception{
		//参数一:被比较的值
		ByteBuffer comparaRowkey =ByteBuffer.allocate(22);
		for (int i = 0; i < 6; i++) {
			comparaRowkey.put(Bytes.toBytes("0"));
		}
		long createTime = format.parse("20120910").getTime();
		comparaRowkey.put(Bytes.toBytes(createTime));
		while (comparaRowkey.hasRemaining()) {
			comparaRowkey.put(Bytes.toBytes("0"));
		}
		//参数二:比较位数设定
		ByteBuffer comparaInfo=ByteBuffer.allocate(22);
		byte zero =0x00;
		byte one =0x01;
		for (int i = 0; i < 6; i++) {
			comparaRowkey.put(one);
		}
		for (int i = 0; i < 6; i++) {
			comparaRowkey.put(zero);
		}
		while (comparaRowkey.hasRemaining()) {
			comparaRowkey.put(one);
		}
		Pair<byte[], byte[]> pair =new Pair<byte[], byte[]>();
		pair.setFirst(comparaRowkey.array());
		pair.setSecond(comparaInfo.array());
		
		List<Pair<byte[], byte[]>> param =new ArrayList<Pair<byte[], byte[]>>();
		param.add(pair);
		//使用param构建fueezyFilter
		FuzzyRowFilter filter =new FuzzyRowFilter(param);
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table .getScanner(scan);
		HBaseUtil.printResultScanner(scanner);
	}
	public static void main(String[] args) throws Exception {
		FilterTest test =new FilterTest();
		//test.searchByCreateTime("20120908");
		//test.searchBy("1","20120904");
		//test.searchByFileName("第1期");
		//test.getPhoneFromPerson();
		test.fuzzyFilterTest();
	}
}
