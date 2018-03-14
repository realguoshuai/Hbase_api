package com.zhiyou.bd20.weibo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zhiyou.bd20.hbasetest.HBaseUtil;

public class UserService {
	private UserDAO userDAO;
	
	
	
	public void setUserDao() {
		this.userDAO = new UserDAO();
		userDAO.setTable();
	}
	//第一题:注册账号,保存人员信息
	public void saveUserInfo(UserEntity user) throws Exception {
		userDAO.putUser(user);
	}
	//第二题:根据账号查询用户信息
	public UserEntity getUserByAccountNo(String accountNo) throws Exception{
		return userDAO.getUserByAccoutNo(accountNo);
	}
	
	//第三题:关注,知道操作人的userId和被操作人的userId(rowkey)
		//需要保存的数据有两条
	public  void focus(String userId,String userIdfocused) throws Exception{
		//场景:用户点击关注 ,在w_user的另一个列簇c中添加f+userIdziduan,值是关注人的rowkey
		
		//获取被关注人的rowkey
		UserEntity focusedUserByAccoutNo = userDAO.getUserByAccoutNo(userIdfocused);
		//System.out.println(focusedUserByAccoutNo.getRowkey()+"1111111111111111111");
		byte[] focusedrowkey = focusedUserByAccoutNo.getRowkey();
		
		//获取关注人的rowkey
		UserEntity userByAccoutNo = userDAO.getUserByAccoutNo(userId.toString());
		byte[] userIdrowkey = userByAccoutNo.getRowkey();
		
		//在关注人rowkey放进去f+userid:f+focusedrowkey

		userDAO.putFocusedUserRowkey(userIdrowkey,focusedrowkey);
		
		//在被关注人rowkey放进去u+userid u+userIdrowkey
		userDAO.putUserRowkey(userIdrowkey,focusedrowkey);
		
		
	}
	
	//第四题:查询某人关注的其他人信息,接受一个userId
	public List<byte[]> getFocusedUserIds(String accountNo) throws Exception{
		//场景:接收输入的用户名 参数 ,获取他的表中f+userid字段的信息
		//使用过滤器 ,比较器
		
		//获取被关注人的rowkey
		UserEntity user = getUserByAccountNo(accountNo);
		byte[] rowKey = user.getRowkey();
		//
		BinaryPrefixComparator comparator = new BinaryPrefixComparator(rowKey);
		RowFilter rfilter = new RowFilter(CompareOp.EQUAL,comparator);
		
		BinaryComparator fComparator = new BinaryComparator(Bytes.toBytes("c"));
		FamilyFilter fFilter = new FamilyFilter(CompareOp.EQUAL, fComparator);
		//定义第二个过滤器:qualierFilter
		BinaryPrefixComparator qComparator = new BinaryPrefixComparator(Bytes.toBytes("f+"));
		QualifierFilter qFilter = new QualifierFilter(CompareOp.EQUAL, qComparator);
		
		FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
		filter.addFilter(rfilter);
		filter.addFilter(fFilter);
		filter.addFilter(qFilter);
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = HBaseUtil.getTable("bd20:w_user").getScanner(scan);
		//HBaseUtil.printResultScanner(resultScanner);
		
		Result result = resultScanner.next();
		List<byte[]> list = new ArrayList<byte[]>();
		if (result != null) {
			while (result.advance()) {
				Cell current = result.current();
				byte[] qualifier = CellUtil.cloneQualifier(current);
				byte[] range = Arrays.copyOfRange(qualifier,2,qualifier.length);
				list.add(range);
			}
		}
		return list;
		
	}

	
	//第五题:查询某人被谁关注,接受一个userId
	public List<byte[]> getFocusUserId(String accountNo) throws Exception{
		//场景:接收输入的用户名 参数 ,获取他的表中f+userid字段的信息
				//使用过滤器 ,比较器
				
				//获取被关注人的rowkey
				UserEntity user = getUserByAccountNo(accountNo);
				byte[] rowKey = user.getRowkey();
				//
				BinaryPrefixComparator comparator = new BinaryPrefixComparator(rowKey);
				RowFilter rfilter = new RowFilter(CompareOp.EQUAL,comparator);
				
				BinaryComparator fComparator = new BinaryComparator(Bytes.toBytes("c"));
				FamilyFilter fFilter = new FamilyFilter(CompareOp.EQUAL, fComparator);
				//定义第二个过滤器:qualierFilter
				BinaryPrefixComparator qComparator = new BinaryPrefixComparator(Bytes.toBytes("u+"));
				QualifierFilter qFilter = new QualifierFilter(CompareOp.EQUAL, qComparator);
				
				FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
				filter.addFilter(rfilter);
				filter.addFilter(fFilter);
				filter.addFilter(qFilter);
				
				
				Scan scan = new Scan();
				scan.setFilter(filter);
				ResultScanner resultScanner = HBaseUtil.getTable("bd20:w_user").getScanner(scan);
				//HBaseUtil.printResultScanner(resultScanner);
				
				Result result = resultScanner.next();
				List<byte[]> list = new ArrayList<byte[]>();
				if (result != null) {
					while (result.advance()) {
						Cell current = result.current();
						byte[] qualifier = CellUtil.cloneQualifier(current);
						byte[] range = Arrays.copyOfRange(qualifier,2,qualifier.length);
						list.add(range);
					}
				}
				return list;
	}
	public static void main(String[] args) throws Exception {
		UserService service=new UserService();
		
		/*
		UserEntity userEntity =new UserEntity();
		userEntity.setAccountNo("user2");
		userEntity.setAge(18);
		userEntity.setGender("女");
		userEntity.setHeadImage("http://dfdfdas/ff.jpg");
		userEntity.setName("Grace");
		userEntity.setPassword("123456");
		userEntity.setPhoneNo("13524643221");
		userEntity.setTimestamp(String.valueOf(new Date().getTime()));
		service.setUserDao();
		service.saveUserInfo(userEntity);
		System.out.println("添加成功");*/
		
		
		/*service.setUserDao();
		UserEntity userByAccountNo = service.getUserByAccountNo("user1");//B@539d019
		System.out.println(userByAccountNo.toString());
		*/
		
		/*service.setUserDao();
		
		service.focus("user1","user2");*/
		
		service.setUserDao();
		List<byte[]> focusedUserIds = service.getFocusedUserIds("user1");
		System.out.println("关注了"+focusedUserIds.toString());
		List<byte[]> getFocusUserId = service.getFocusedUserIds("user2");
		System.out.println("被"+getFocusUserId.toString()+"关注了");
		
		
	//	service.getUserInfo("user1");
		
		
		
	}
}
