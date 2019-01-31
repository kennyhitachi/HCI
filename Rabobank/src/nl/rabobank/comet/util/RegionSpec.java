package nl.rabobank.comet.util;

import java.util.TimeZone;

import com.hds.hcp.apihelpers.HCPUtils;

public class RegionSpec {
	String mIdentifier;
	String mNamespace;
	String mTenant;
	String mHCPName;
	String mUserName;
	String mEncodedPassword;
	TimeZone mTimeZone;

	RegionSpec(String inIdentifier) {
		mIdentifier = inIdentifier;
	}
	
	public String getIdentifier() { return mIdentifier; }
	
	public String getNamespace() { return mNamespace; }
	public void setNamespace(String inNamespace) { mNamespace = inNamespace; }
	
	public String getTenant() { return mTenant; }
	public void setTenant(String inTenant) { mTenant = inTenant; }
	
	public String getHCPName() { return mHCPName; }
	public void setHCPName(String inHCPName) { mHCPName = inHCPName; }
	
	public String getEncodedUserName() { 
		return HCPUtils.toBase64Encoding(mUserName);
	}
	
	public String getUserName() { return mUserName; }
	public void setUserName(String inUserName) { mUserName = inUserName; }

	public String getEncodedPassword() { return mEncodedPassword; }
	public void setEncodedPassword(String inEncodedPassword) { mEncodedPassword = inEncodedPassword; }
	
	public TimeZone getTimeZone() { return mTimeZone; }
	public void setTimeZone(TimeZone inTimeZone) { mTimeZone = inTimeZone; }
	
	public void setValues(String inNamespace, String inTenant, String inHCPName, String inUserName, String inEncodedPassword, TimeZone inTimeZone ) {
		mNamespace = inNamespace;
		mTenant = inTenant;
		mHCPName = inHCPName;
		mUserName = inUserName;
		mEncodedPassword = inEncodedPassword;
		mTimeZone = inTimeZone;
	}
	
	public boolean isMatch(String inIdentifier) {
		return mIdentifier.equals(inIdentifier);
	}
	
	public String toString() {
		StringBuilder outString = new StringBuilder();
		
		outString.append("Identifier: " + getIdentifier() + "\n");
		outString.append("Namespace:  " + getNamespace() + "\n");
		outString.append("Tenant:     " + getTenant() + "\n");
		outString.append("HCPName:    " + getHCPName() + "\n");
		outString.append("UserName:   " + getUserName() + "\n");
		outString.append("EncodedPwd: " + getEncodedPassword() + "\n");
		outString.append("TimeZone:   " + getTimeZone() + "\n");
		
		return outString.toString();
	}
}
