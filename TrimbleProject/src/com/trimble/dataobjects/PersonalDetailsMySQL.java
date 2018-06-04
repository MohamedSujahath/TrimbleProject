package com.trimble.dataobjects;

public class PersonalDetailsMySQL {
	
	private Integer uid;
	private String id;
	private String name;
	private String telephone;
	private String address;
	private String age;
	private String queueMsg;
	
	
	
	public Integer getUid() {
		return uid;
	}
	public void setUid(Integer uid) {
		this.uid = uid;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getTelephone() {
		return telephone;
	}
	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	
	
	
	public String getQueueMsg() {
		return queueMsg;
	}
	public void setQueueMsg(String queueMsg) {
		this.queueMsg = queueMsg;
	}
	@Override
	public String toString() {
	    return "uid: " + this.getUid() + 
	           ", id: " + this.getId() + 
	           ", name: " + this.getName() + 
	           ", telephone: " + this.getTelephone() + 
	           ", address: " + this.getAddress() + 
	           ", age: " + this.getAge();
	}

}
