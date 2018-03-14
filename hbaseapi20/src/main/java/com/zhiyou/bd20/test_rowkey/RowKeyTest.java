package com.zhiyou.bd20.test_rowkey;

import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyTest {
	//生成rowkey
	//UserId+createTime+fileId
	//6+8+8
	//rowkey的长度是22字节
	public static byte[] getRowKey(long userId,long creatTime,
			long fileId){
		//定义一个容纳22字节的byteBuffer,来容纳rowkey
		ByteBuffer buffer = ByteBuffer.allocate(22);
		//定义一个容纳6字节的byteBuffer来容纳userid
		ByteBuffer userIdbuffer = ByteBuffer.allocate(6);
		//为了散列userId需要逆向
		String userIdStrRev = StringUtils.reverse(String.valueOf(userId));
		userIdbuffer.put(Bytes.toBytes(userIdStrRev));
		//如果userIdbuffer没填满,则在后面补0
		while (userIdbuffer.hasRemaining()) {
			userIdbuffer.put(Bytes.toBytes("0"));
		}
		//检验是否正确
		System.out.println(Bytes.toString(userIdbuffer.array()));
		byte[] ctime = Bytes.toBytes(creatTime);
		System.out.println("ctime长度:"+ctime.length);
		//把三部分按照顺序组装到buffer中
		buffer.put(userIdbuffer.array());
		buffer.put(Bytes.toBytes(creatTime));
		buffer.put(Bytes.toBytes(fileId));
		return buffer.array();
	}
	public static void main(String[] args) {
		getRowKey(111,2,3);
	}
}
