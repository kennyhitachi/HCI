package com.hitachi.hci.plugins.stage.utils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CombineMatches {
    // Custom function to combine the matches based on the regular expression
    public static ArrayList<String> combineMatches (String stringSource, String stringRegEx) {
	    Pattern pattern = Pattern.compile(stringRegEx);
	    Matcher matcher = pattern.matcher(stringSource);
	    ArrayList<String> combinedValuesList = new ArrayList<String>();
	    while (matcher.find()) {
	    	combinedValuesList.add(matcher.group());
	    }	
	    return combinedValuesList;
    }
}
