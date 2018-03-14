package com.zhiyou.bd20.test_rowkey;

import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.zhiyou.bd20.hbasetest.HBaseUtil;

public class InputSameData {
	private SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
	private Table table;

	public InputSameData() {
		table = HBaseUtil.getTable("bd20:file_info");
	}

	// 定义一个方法 把实例中的数据插入到hbase中
	// 表:create 'bd20:file_info','i'
	// 对应字段列名称:id:1,CreateTime:20120904,Name:中国好声音第1期
	// Category:综艺,UserID;1
	public void putSameData(String id, String createTime, String name, String category, String userId)
			throws Exception {
		byte[] rowKey = RowKeyTest.getRowKey(Long.valueOf(userId), format.parse(createTime).getTime(),
				Long.valueOf(id));
		Put put = new Put(rowKey);
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("id"), Bytes.toBytes(id));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("createTime"), Bytes.toBytes(createTime));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("name"), Bytes.toBytes(name));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("category"), Bytes.toBytes(category));
		;
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("userId"), Bytes.toBytes(userId));
		;

		table.put(put);
	}

	// 查询表中userId=1,查询范围20120209-20120910的数据
	// 如果rowkey能跟查询条件限定关系 就用rowkey
	public void searchByUserDate(String userId, String startDate, String stopDate) throws Exception {
		byte[] startRowkey = RowKeyTest.getRowKey(Long.valueOf(userId), format.parse(startDate).getTime(),
				Long.MIN_VALUE);
		byte[] stopRowkey = RowKeyTest.getRowKey(Long.valueOf(userId), format.parse(stopDate).getTime(),
				Long.MAX_VALUE);
//		byte[] stopRowkey = RowKeyTest.getRowKey(Long.valueOf(userId), format.parse(stopDate).getTime(),
//				01);

		Scan scan = new Scan();
		scan.setStartRow(startRowkey);
		scan.setStopRow(stopRowkey);
		
		ResultScanner scanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(scanner);
	}

	
	public static void main(String[] args) throws Exception {
		InputSameData iData = new InputSameData();
		/*iData.putSameData("1", "20120902", "中国好声音第1期", "综艺", "1");
		iData.putSameData("2", "20120904", "中国好声音第2期", "综艺", "1");
		iData.putSameData("3", "20120906", "中国好声音外卡赛", "综艺", "1");
		iData.putSameData("3", "20120908", "中国好声音第3期", "综艺", "1");
		iData.putSameData("4", "20120910", "中国好声音第4期", "综艺", "1
		");
		iData.putSameData("5", "20120912", "中国好声音选手采访", "综艺花絮", "2");
		iData.putSameData("7", "20120914", "中国好声音第5期", "综艺", "1");
		iData.putSameData("8", "20120916", "中国好声音录制花絮", "综艺花絮", "2");
		iData.putSameData("9", "20120918", "张玮独家专访", "花絮", "3");
		iData.putSameData("10", "20120920", "加多宝凉茶广告", "综艺广告", "4");*/
		
		
		
		iData.searchByUserDate("1", "20120901", "20120906");
	}
}
