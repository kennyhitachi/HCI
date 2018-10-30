package com.hitachi.hci.aw.utils;

/*
 * {  
   "type":"FILE",
   "parent":"/test/hci/license",
   "name":"CFN.plk",
   "changeTime":1540395148082,
   "size":1938,
   "hash":"d08ea3137e194afa13660b1264d5f726380fced2dad24107151898c2f35acba8976bc994ac3e148cccb584b8d86bb323",
   "access":"COLLABORATOR",
   "etag":"EAhevySfQ9Q3DrlFw2u7r2M3FSTehjzdKXpfqAqO4HM/8Fk5Pjpr6VJHqjemlI05Yc4CfGDRwlJodon2kYTMI7If0IFf3S1k0RKgjYpjfiEKDzzK1VBtJsdx7V+a/RBlge+Ty1eDyUCGxT5trpaFgA\u003d\u003d",
   "state":"CREATE"
}
 */
public class AWFileUploadResponse {
   public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getParent() {
		return parent;
	}
	public void setParent(String parent) {
		this.parent = parent;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getChangeTime() {
		return changeTime;
	}
	public void setChangeTime(long changeTime) {
		this.changeTime = changeTime;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public String getHash() {
		return hash;
	}
	public void setHash(String hash) {
		this.hash = hash;
	}
	public String getAccess() {
		return access;
	}
	public void setAccess(String access) {
		this.access = access;
	}
	public String getEtag() {
		return etag;
	}
	public void setEtag(String etag) {
		this.etag = etag;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
private String type;
   private String parent;
   private String name;
   private long changeTime;
   private int size;
   private String hash;
   private String access;
   private String etag;
   private String state;
   
}
