package com.zhiyou.bd20.weibo;

import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.zhiyou.bd20.hbasetest.HBaseUtil;


public class WeiBoUtils {
	public static final byte BLANK_STR=0x1f;
	public static final byte ZERO=0x00;
	public static final byte ONE=0x01;
	
	public static byte[] getUserRowKey(String accountNo,String phoneNo) throws Exception {
		//1.获取唯一标识pk
		//调用getSequenceByTableName,传'w_user'为参数,获取w_user的递增序列
		long sequence =getSequenceByTableName("w_user");
		//获取到的值,转成字符串,逆向
		String pk = StringUtils.reverse(String.valueOf(sequence));
		//然后根据长度在后面补'0'
		int coverNum =12-pk.length();
		//唯一标识pk
		pk=coverLeft(pk, '0', coverNum);
		//2.对accountNo限定或补长 补0x1f,在左边补
		byte[] account =coverLeft(accountNo, 17);
		//3.对phoneNo限定长度或补长 补0x1f
		byte[] phone =coverLeft(phoneNo, 11);
		//4.组装上面三个获得rowkey
		ByteBuffer buffer = ByteBuffer.allocate(40);
		buffer.put(Bytes.toBytes(pk));
		buffer.put(account);
		buffer.put(phone);
		return buffer.array();
	}
	
	//从sequence表中获取递增序列
	public static long getSequenceByTableName(String tableName) throws Exception {
		//对 bd20:sequence表调用incr指令,rowkey是tName,获取到的sequence ,转成long返回
		Table sequence =HBaseUtil.getTable("bd20:sequence");
		long result = sequence.incrementColumnValue(Bytes.toBytes("tableName")
					,Bytes.toBytes("i"),Bytes.toBytes("s"),2);
		return result;
	}
	//把给定的字符串补另一个字符串,补N次
	public static String coverLeft(String source,char cover,int times){
		while (times>0) {
			source+=cover;
			times -=1;
		}
		return source;
	}
	//把给定的字符串转化成字节数组后,空白的在左边补0xlf(空)
	public static byte[] coverLeft(String source,int length){
		ByteBuffer buffer =ByteBuffer.allocate(length);
		buffer.put(Bytes.toBytes(source));
		while (buffer.hasRemaining()) {
			buffer.put(BLANK_STR);
		}
		return buffer.array();
	}
	//给ByteBuffer后面补N个字节
	public static void coverLeft(ByteBuffer source,int length,byte cover){
		while (length>0) {
			source.put(cover);
			length-=1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		/*long result = getSequenceByTableName("w_user");
		System.out.println(result);*/
		byte[] rowkey =getUserRowKey("aaahgjgh","12345678910");
		System.out.println(rowkey.length);
		System.out.println(Bytes.toString(rowkey));
	}
}
