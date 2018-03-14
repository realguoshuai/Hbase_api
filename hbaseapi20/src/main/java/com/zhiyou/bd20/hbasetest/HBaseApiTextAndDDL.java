package com.zhiyou.bd20.hbasetest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.replication.ReplicationPeerZKImpl.TableCFsTracker;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseApiTextAndDDL {
	
	private Configuration conf = HBaseConfiguration.create();
	private Connection connection;
	private Admin admin;
	
	public HBaseApiTextAndDDL() throws Exception {
		connection=ConnectionFactory.createConnection(conf);
		admin =connection.getAdmin();
	}

	//创建一个hbase表
	public void CreateTable(String tName,String... colFamily){
		//创建htabledescribe对象
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tName));	
		for (String cf : colFamily) {
			desc.addFamily(new HColumnDescriptor(cf));
			
		}
		try {
			admin.createTable(desc);
			System.out.println("succ");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("fail");
		}
	}
	
	
		//删除一个hbase表
		public void deleteTable(String tName){
			//包含了表的名字极其对应表的列族
			HTableDescriptor hTableDescriptor = 
						new HTableDescriptor(TableName.valueOf(tName));
			TableName tableName = hTableDescriptor.getTableName();
			try {
				//删除表时需要先disable
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("删除表"+tableName+"成功");
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("删除表遇到错误");
			}
		}
		//创建一个namespace(list_namespace查看,namespace就是数据库名)
		public void createNamespace(String namespace){
			NamespaceDescriptor nDescriptor
				= NamespaceDescriptor.create(namespace).build();
			
			try {
				admin.createNamespace(nDescriptor);
				System.out.println("创建成功");
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("创建失败");
			}
		}
		//打印出hbase中有多少张表的表名信息
		public void printTables(){
			
			try {
				
				HTableDescriptor[] listTables = admin.listTables();
				int count =0;
				for (HTableDescriptor ht: listTables) {
					count++;
					System.out.println(ht);
				}
				System.out.println("hbase下一共有"+count+"表");
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("查询表名出错了");
			}
		}
		//判断一个表是否在hbase中存在
		public void isTableExists(String tName){
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tName));
			try {
				boolean tableExists = admin.tableExists(desc.getTableName());
				if (tableExists) {
					System.out.println("表在hbase中存在");
				}else{
					System.out.println("表在hbase中不存在");
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("出错了吧");
			}
			
		}
		//打印出一个namespace下所有的表名信息
		public void listNamespaceTables(String namespace){
			
			try {
				TableName[] names = admin.listTableNamesByNamespace(namespace);
				for (TableName tableName : names) {
					System.out.println(namespace+"下 有"+tableName);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("程序出错了");
			}
		}
		//打印出给定表的详细信息
		public void describeTable(String tName){
			try {
				HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(tName));
				System.out.println(descriptor);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("程序出错了");
			}
			
		}
		
	
	
	
	
	//断开连接
	public void close(){
		if (connection!=null) {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws Exception {
		//创建一张表
		HBaseApiTextAndDDL apiTest = new HBaseApiTextAndDDL();
		//apiTest.CreateTable("bd20:fromjava", "i","u");
		//apiTest.deleteTable("aaa");
		//apiTest.createNamespace("ccc");
		//apiTest.printTables();
		//apiTest.isTableExists("bd20:fromjava");
		//apiTest.listNamespaceTables("default");
		apiTest.describeTable("bd20:fromjava");
		apiTest.close();
	}
}
