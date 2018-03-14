package com.zhiyou.bd20.hbasetest;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
	public static final Configuration CONFIGURATION = HBaseConfiguration.create();
	public static Connection connection;

	// 获取表连接
	public static void setConnection() {
		try {
			connection = ConnectionFactory.createConnection(CONFIGURATION);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("连接Hasee失败");
		}

	}

	// 如果执行ddl,需要admin对象
	public static Admin getAdmin() {
		if (connection == null) {
			setConnection();
		}
		try {
			return connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("获取admin对象失败");
			return null;
		}
	}

	// 如果需要执行dml,需要table对象
	// 通用的获取table对象的方法
	public static Table getTable(String tNmae) {
		if (connection == null) {
			setConnection();
		}
		try {
			return connection.getTable(TableName.valueOf(tNmae));
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("获取表" + tNmae + "失败");
			return null;
		}
	}

	// 对给定的result对象进行遍历,并打印其中的内容:rowkey,family,qualify,timastamp,value
	public static void printResult1(Result result) {
		// 使用advance 和current方法
		while (result.advance()) {
			Cell cell = result.current();
			byte[] rowkey = CellUtil.cloneRow(cell);
			byte[] family = CellUtil.cloneFamily(cell);
			byte[] quality = CellUtil.cloneQualifier(cell);
			byte[] value = CellUtil.cloneValue(cell);
			long ts = cell.getTimestamp();
			System.out.println("rowkey: " + Bytes.toString(rowkey) + " family: " + Bytes.toString(family) + " quality: "
					+ Bytes.toString(quality) + " timespace; " + ts + " value: " + Bytes.toString(value));
		}
	}

	public static void printResult2(Result result) {
		CellScanner scanner = result.cellScanner();
		// printResult1是对result中数据的一次性消费,printResult2可以使用多次
		try {
			while (scanner.advance()) {
				Cell cell = result.current();
				byte[] rowkey = CellUtil.cloneRow(cell);
				byte[] family = CellUtil.cloneFamily(cell);
				byte[] quality = CellUtil.cloneQualifier(cell);
				byte[] value = CellUtil.cloneValue(cell);
				long ts = cell.getTimestamp();
				System.out.println("rowkey" + Bytes.toString(rowkey) + " family" + Bytes.toString(family) + " quality"
						+ Bytes.toString(quality) + " timespace" + ts + " value" + Bytes.toString(value));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void printResult3(Result result) {
		// Result是一个游标
		List<Cell> list = result.listCells();
		for (Cell cell : list) {
			byte[] rowkey = CellUtil.cloneRow(cell);
			byte[] family = CellUtil.cloneFamily(cell);
			byte[] quality = CellUtil.cloneQualifier(cell);
			byte[] value = CellUtil.cloneValue(cell);
			long ts = cell.getTimestamp();
			System.out.println("rowkey" + Bytes.toString(rowkey) + " family" + Bytes.toString(family) + " quality"
					+ Bytes.toString(quality) + " timespace" + ts + " value" + Bytes.toString(value));

		}
	}

	// 定义一个方法把scan结果resultScanner,按照上述样式打印出来
	public static void printResultScanner(ResultScanner resultScanner) {
		try {
			Result result = resultScanner.next();
			while (result!=null) {
				printResult3(result);
				result = resultScanner.next();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
