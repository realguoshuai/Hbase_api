package com.zhiyou.bd20.test_rowkey;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Page {

	private static Table table = com.zhiyou.bd20.hbasetest.HBaseUtil.getTable("bd20:file_info");
	//展示数据
	public static void getResult(Result result) throws Exception {
		
		List<Cell> list = result.listCells();
		for (Cell cells : list) {
			byte[] rowkey = CellUtil.cloneRow(cells);
			byte[] family = CellUtil.cloneFamily(cells);
			byte[] quailfy = CellUtil.cloneQualifier(cells);
			byte[] value = CellUtil.cloneValue(cells);

			System.out.println("rowkey: "+Bytes.toString(rowkey)+
					" family: "+Bytes.toString(family)+
					" quailfy: "+Bytes.toString(quailfy)+
					" value: "+Bytes.toString(value));
		}
	}
	//分页
	public static void getResultByPage(int pageIndex,int pageSize,ResultScanner resultScanner) throws Exception{

		Result result = resultScanner.next();
		
		int count=0;
		
		while (result != null) {
			if (count++<(pageIndex - 1) * pageSize || count>(pageIndex - 1) * pageSize+pageSize) {
				result = resultScanner.next();
				continue;
			}
			getResult(result);
		
			result = resultScanner.next();
		}
		
	}
	
	public static void showInfo(int pageIndex,int pageSize) throws Exception {
		Scan scan  = new Scan();
		ResultScanner resultSca = table.getScanner(scan);
		System.out.println(resultSca+"-------111111111");
		getResultByPage(pageIndex, pageSize,resultSca);
	}
	
	public static void main(String[] args) throws Exception {
		showInfo(2, 2);
	}
}
