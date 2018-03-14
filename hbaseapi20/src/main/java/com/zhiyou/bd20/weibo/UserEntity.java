package com.zhiyou.bd20.weibo;

public class UserEntity {
	private  byte[] rowkey;
	private String name;
	private String gender;
	private int age;
	private String phoneNo;
	private String accountNo;
	private String password;
	private String headImage;
	private String timestamp;

	public UserEntity() {
		super();
	}

	public UserEntity(byte[] rowkey, String name, String gender, int age, String phoneNo, String accountNo,
			String password, String headImage, String timestamp) {
		super();
		this.rowkey = rowkey;
		this.name = name;
		this.gender = gender;
		this.age = age;
		this.phoneNo = phoneNo;
		this.accountNo = accountNo;
		this.password = password;
		this.headImage = headImage;
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "UserEntity [rowkey=" + rowkey + ", name=" + name + ", gender=" + gender + ", age=" + age + ", phoneNo="
				+ phoneNo + ", accountNo=" + accountNo + ", password=" + password + ", headImage=" + headImage
				+ ", timestamp=" + timestamp + "]";
	}

	public byte[] getRowkey() {
		return rowkey;
	}

	public void setRowkey(byte[] rowkey2) {
		this.rowkey = rowkey2;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getPhoneNo() {
		return phoneNo;
	}

	public void setPhoneNo(String phoneNo) {
		this.phoneNo = phoneNo;
	}

	public String getAccountNo() {
		return accountNo;
	}

	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHeadImage() {
		return headImage;
	}

	public void setHeadImage(String headImage) {
		this.headImage = headImage;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

}
