package com.hitachi.hci.aw.utils;

import java.util.List;

/*
 * {  
   "path":"/Team-Projects",
   "expirationDays":30,  -- optional
   "public":true,        -- optional
   "accessCode":true,
   "permissions":[  
      'READ'
   ]
}
 */
public class AWCreateLinkRequest {
 
	public AWCreateLinkRequest(String path, boolean accessCode, List<String> permissions) {
		this.path = path;
		this.accessCode = accessCode;
		this.permissions = permissions;
	}
	private String path;
	private boolean accessCode;
	private List<String> permissions;
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public boolean isAccessCode() {
		return accessCode;
	}
	public void setAccessCode(boolean accessCode) {
		this.accessCode = accessCode;
	}
	public List<String> getPermissions() {
		return permissions;
	}
	public void setPermissions(List<String> permissions) {
		this.permissions = permissions;
	}
}
