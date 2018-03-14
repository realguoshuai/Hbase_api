package com.zhiyou.bd20.weibo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.zhiyou.bd20.hbasetest.HBaseUtil;

public class UserDAO {
	private Table table;

	public void setTable() {
		this.table = HBaseUtil.getTable("bd20:w_user");
	}

	// 添加用户
	public void putUser(UserEntity userEntity) throws Exception {
		// 判断是否新增userEntity的rowkey是否为空个,若为空生成并set进来
		if (userEntity.getRowkey() == null) {
			byte[] rowkey = WeiBoUtils.getUserRowKey(userEntity.getAccountNo(), userEntity.getPhoneNo());
			userEntity.setRowkey(rowkey);
		}
		// 定义Put对象
		Put put = new Put(userEntity.getRowkey());
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("name"), Bytes.toBytes(userEntity.getName()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("gender"), Bytes.toBytes(userEntity.getGender()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"), Bytes.toBytes(userEntity.getAge()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("phoneNo"), Bytes.toBytes(userEntity.getPhoneNo()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("AccountNo"), Bytes.toBytes(userEntity.getAccountNo()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("password"), Bytes.toBytes(userEntity.getPassword()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("headImage"), Bytes.toBytes(userEntity.getHeadImage()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("timestamp"), Bytes.toBytes(userEntity.getTimestamp()));
		table.put(put);
	}
	
	//根据姓名查询
	public UserEntity getUserByAccoutNo(String accountNo) throws Exception {
		// 使用FuzzyRowFilter匹配
		ByteBuffer account = ByteBuffer.allocate(17);
		account.put(Bytes.toBytes(accountNo));

		while (account.hasRemaining()) {
			account.put(WeiBoUtils.BLANK_STR);
		}
		ByteBuffer companyRowkey = ByteBuffer.allocate(40);
		WeiBoUtils.coverLeft(companyRowkey, 12, WeiBoUtils.ZERO);
		companyRowkey.put(account.array());
		WeiBoUtils.coverLeft(companyRowkey, 11, WeiBoUtils.ZERO);

		ByteBuffer companyInfo = ByteBuffer.allocate(40);
		WeiBoUtils.coverLeft(companyInfo, 12, WeiBoUtils.ONE);
		WeiBoUtils.coverLeft(companyInfo, 17, WeiBoUtils.ZERO);
		WeiBoUtils.coverLeft(companyInfo, 11, WeiBoUtils.ONE);

		Pair<byte[], byte[]> fuzzyParam = new Pair<byte[], byte[]>();
		fuzzyParam.setFirst(companyRowkey.array());
		fuzzyParam.setSecond(companyInfo.array());

		List<Pair<byte[], byte[]>> fuzzyParamList = new ArrayList<Pair<byte[], byte[]>>();
		fuzzyParamList.add(fuzzyParam);

		Filter filter = new FuzzyRowFilter(fuzzyParamList);
		Scan scan = new Scan();
		scan.setFilter(filter);

		ResultScanner resultScanner = table.getScanner(scan);
		Result result = resultScanner.next();
		if (result != null) {
			UserEntity userEntity = new UserEntity();
			userEntity.setRowkey(result.getRow());
			userEntity.setAccountNo(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("AccountNo"))));
			userEntity.setAge(Bytes.toInt(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("age"))));
			userEntity.setGender(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("gender"))));
			userEntity.setHeadImage(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("headImage"))));
			userEntity.setName(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("name"))));
			userEntity.setPassword(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("password"))));
			userEntity.setPhoneNo(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("phoneNo"))));
			userEntity.setTimestamp(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("timestamp"))));

			return userEntity;
		} else {
			return null;
		}
	}
	
	//点击关注后,添加被关注用户的rowkey到 w_user表的列簇c的  f+userid下
	public void putFocusedUserRowkey(byte[] userIdrowkey,byte[] focusedrowkey) throws Exception {

		// 定义Put对象
		Put put = new Put(userIdrowkey);
		put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("f+"+focusedrowkey),Bytes.toBytes("0"));
		table.put(put);
	}
	//点击关注后,添加被关注用户的rowkey到 w_user表的列簇c的  u+userid下
	public void putUserRowkey(byte[] userIdrowkey,byte[] focusedrowkey) throws Exception {
		
		// 定义Put对象
		Put put = new Put(focusedrowkey);
		put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("u+"+userIdrowkey),Bytes.toBytes("0"));
		table.put(put);
	}
	
	
}
