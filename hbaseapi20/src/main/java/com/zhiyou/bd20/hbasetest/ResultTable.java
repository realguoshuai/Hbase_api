package com.zhiyou.bd20.hbasetest;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ResultTable {
	private Table table = HBaseUtil.getTable("bd20:fromjava");
	
	private void getSomerResult() {
		Get get = new Get(Bytes.toBytes("rowkey3"));
		try {
			Result result = table.get(get);
			HBaseUtil.printResult1(result);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("获取数据失败");
		}
	}
	
	//scan
	public void scanTable(){
		Scan scan= new Scan();
		try {
			ResultScanner resultScanner =table.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
		public static void main(String[] args) {
			ResultTable table = new ResultTable();
			table.getSomerResult();
		}	
}
