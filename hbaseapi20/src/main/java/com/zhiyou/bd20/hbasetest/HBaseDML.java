package com.zhiyou.bd20.hbasetest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

//对表bd20:fromjava进行dml操作
public class HBaseDML {
	private Table table;

	public HBaseDML() {
		this.table = HBaseUtil.getTable("bd20:fromjava");
	}

	// 向bd20:fromjava里面插入数据,修改数据
	public void writeDataToTable() {
		// 创建put对象
		Put put = new Put(Bytes.toBytes("1"));

		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("col1"), Bytes.toBytes("change_value"));
		put.addColumn(Bytes.toBytes("u"), Bytes.toBytes("ucol2"), Bytes.toBytes(222222));

		try {
			table.put(put);
			System.out.println("数据更新成功");
		} catch (IOException e) {
			System.out.println("数据保存失败");
			e.printStackTrace();
		}
	}

	// 删除数据
	public void deleteDataFromTable() {
		Delete delete = new Delete(Bytes.toBytes("1"));
		// delete.addFamily(Bytes.toBytes("i")) //删除列簇
		delete.addColumn(Bytes.toBytes("u"), Bytes.toBytes("ucol1"));
		try {
			table.delete(delete);
			System.out.println("删除数据成功");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("删除数据失败");

		}
	}

	// 从hbase中获取数据 get
	public void getDataFromTable() {
		// 创建对象
		Get get = new Get(Bytes.toBytes("1"));
		try {
			Result result = table.get(get);
			byte[] ucol2 = result.getValue(Bytes.toBytes("u"), Bytes.toBytes("ucol2"));
			System.out.println("获取到数据" + Bytes.toInt(ucol2));
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("获取数据失败");
		}
	}

	// 使用scan (全表扫描)
	public void scanDataFromTable() {
		// 创建scan对象通过scan对象的设置,来有条件的扫描
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("u"));
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			Result result = resultScanner.next();
			while (result != null) {
				System.out.print("rowkey:" + Bytes.toString(result.getRow()) + ",u:ucol1,value:"
						+ Bytes.toString(result.getValue(Bytes.toBytes("u"), Bytes.toBytes("ucol1"))));
				result = resultScanner.next();
			}
			System.out.println();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("扫描数据失败");
		}

	}

	// 定义一个方法，往hbase表中批量插入5行数据
	public void batchInsertData() {
		List<Put> list = new ArrayList<Put>();
		for (int i = 0; i < 5; i++) {
			Put put = new Put(Bytes.toBytes("rowkey"+i));
			put.addColumn(Bytes.toBytes("u"), Bytes.toBytes("ucol"+i), Bytes.toBytes(i));
			list.add(put);
		}
		try {
			table.put(list);
			System.out.println("数据批量添加成功");
		} catch (IOException e) {
			System.out.println("数据保存失败");
			e.printStackTrace();
		}
	}

	// 定义一个方法，把hbase表中的数据批量删除3行数据
	public void batchDeleteData() {
		List<Delete> list = new ArrayList<Delete>();
		for (int i = 0; i < 3; i++) {
			Delete delete = new Delete(Bytes.toBytes("rowkey"+i));
			delete.addColumn(Bytes.toBytes("u"), Bytes.toBytes("ucol"+i));
			list.add(delete);
		}
		try {
			table.delete(list);
			System.out.println("删除数据成功");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("删除数据失败");

		}
	}

	// 定义一个方法，从hbase表中批量get 3行数据（打印出来）
	public void batchGetData() {
		List<Get> list = new ArrayList<Get>();
		for (int i = 0; i < 3; i++) {
			Get get = new Get(Bytes.toBytes(i));
			list.add(get);
			
			try {
				Result[] result = table.get(list);
				for (Result result2 : result) {
					
					byte[] ucol2 = result2.getValue(Bytes.toBytes("u"), Bytes.toBytes("ucol"+i));
					System.out.println("获取到数据" + Bytes.toInt(ucol2));
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("获取数据失败");
			}
		}
	}

	// 定义一个方法，从hbase指定某个列簇下的某个列成员名称，扫描出多行数据的结果（打印出来）
	public void batchScanData() {
		Scan scan = new Scan();
		try {
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
			    System.out.println("\n row: "+new String(result.getRow()));
			    //for(KeyValue kv:r.raw()){
			    	
			    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	//get
	
	
	public static void main(String[] args) {
		HBaseDML hBaseDML = new HBaseDML();
		// hBaseDML.writeDataToTable();
		// hBaseDML.deleteDataFromTable();
		//hBaseDML.getDataFromTable();
		// hBaseDML.scanDataFromTable();
		
		//hBaseDML.batchInsertData();
		//hBaseDML.batchDeleteData();
		hBaseDML.batchGetData();
	}
}
