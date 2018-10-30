package com.hitachi.hci.aw.utils;

/*
 * {
    "url": "https://anywhere.example.com/u/79KZnP1SmZT90pmc/Work-Projects?l",
    "path": "/Work Projects",
    "expirationDate": 1459440864685,
    "public": true,
    "accessible":true,
    "accessCode": "8h;nDy0z",
    "permission": ["READ"],
    "token": "79KZnP1SmZT90pmc",
    "type":"FOLDER",
    "itemName": "Work Projects"
}
 */

public class AWCreateLinkResponse {
 private String url;
 private String path;
 private long expirationDate;
 private boolean accessible;
 private String accessCode;
 private String permission;
 private String token;
 private String type;
 private String itemName;
public String getUrl() {
	return url;
}
public void setUrl(String url) {
	this.url = url;
}
public String getPath() {
	return path;
}
public void setPath(String path) {
	this.path = path;
}
public long getExpirationDate() {
	return expirationDate;
}
public void setExpirationDate(long expirationDate) {
	this.expirationDate = expirationDate;
}
public boolean getAccessible() {
	return accessible;
}
public void setAccessible(boolean accessible) {
	this.accessible = accessible;
}
public String getAccessCode() {
	return accessCode;
}
public void setAccessCode(String accessCode) {
	this.accessCode = accessCode;
}
public String getPermission() {
	return permission;
}
public void setPermission(String permission) {
	this.permission = permission;
}
public String getToken() {
	return token;
}
public void setToken(String token) {
	this.token = token;
}
public String getType() {
	return type;
}
public void setType(String type) {
	this.type = type;
}
public String getItemName() {
	return itemName;
}
public void setItemName(String itemName) {
	this.itemName = itemName;
}
}
