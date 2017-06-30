package com.hds.hci.plugins.stage.utils;

public class StringUtil {
	
	public static String removeExtension(String inFileName) {
		String outFileName="";
		if (inFileName.indexOf(".") > 0) {
			outFileName = inFileName.substring(0, inFileName.lastIndexOf("."));
		}
		return outFileName;
	}

}
