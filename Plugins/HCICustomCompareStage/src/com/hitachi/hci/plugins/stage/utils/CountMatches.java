package com.hitachi.hci.plugins.stage.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CountMatches {
    // Custom function to count the matches based on the regular expression or text or delimiter
    public static int countMatches (String stringSource, String stringRegEx) {
	    Pattern pattern = Pattern.compile(stringRegEx);
	    Matcher matcher = pattern.matcher(stringSource);
	    int count = 0;
	    while (matcher.find()) {
	        count++;
	    }
	    return count;
    }
}
