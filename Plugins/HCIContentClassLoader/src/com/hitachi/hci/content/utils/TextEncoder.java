package com.hitachi.hci.content.utils;

import java.util.ArrayList;
import java.util.Random;


public class TextEncoder {

    private static Object btLock = new Object();
    private static String mMappingTable = null;

    // Two important aspects.  Intentionally 
    private static void loadMapping() {
        synchronized(btLock) {
            if (null == mMappingTable) {
                StringBuilder table = new StringBuilder();
                for (char i = 'A'; i <= 'Z'; i++) {
                    table.append(i);
                }
                table.append(table.toString().toLowerCase());
                for (int i = 0; i <= 9; i++) {
                    table.append(Integer.toString(i));
                }
                table.append("+/");
                
                mMappingTable = table.toString(); 
            }
        }
    }

    private static ArrayList<Integer> buildArray(char inSeedChar, int inLength) {
        Random generator = new Random(inSeedChar);
        ArrayList<Integer> list = new ArrayList<Integer>();

        // Keep building array list until we have enough unique numbers.
        while (list.size() < inLength) {
            int nextDigit = generator.nextInt(inLength);
            
            if (! list.contains(nextDigit)) {
                list.add(nextDigit);
            }
        }
        
        return list;
    }
    
    //
    //  Encode/decode algorithm.
    //      Encoded content is base64 with scrambling.
    
    //      First character of encoded content is a seed.
    //      Seed represented as a single base64 allowed character
    //      used to determine the order of the actual base64 encoded
    //      string that is the real text.
    
    
    static public String encode(String inContent) {
        if (null == inContent || inContent.isEmpty()) {
            return null;
        }
        
        loadMapping();
        
        String basicEncodedString = HCPUtils.toBase64Encoding(inContent);
        
        char seedChar = mMappingTable.charAt(new Random(System.currentTimeMillis()).nextInt(mMappingTable.length()-1));

        StringBuilder retVal = new StringBuilder();
        
        retVal.append(seedChar);

        ArrayList<Integer> list = buildArray(seedChar, basicEncodedString.length());
        
        // Using the order list now scramble the characters.
        for (int index : list) {
            retVal.append(basicEncodedString.charAt(index));
        }
        
        return retVal.toString();
    }
    
    static public String decode(String inEncodedContent) {
        if (null == inEncodedContent || inEncodedContent.isEmpty()) {
            return null;
        }
        
        loadMapping();
        
        char seedChar = inEncodedContent.charAt(0);
        
        ArrayList<Integer> list = buildArray(seedChar, inEncodedContent.length()-1);
        
        char rawBytes[] = new char[inEncodedContent.length()-1];
        
        // Using the order list now unscramble the characters.
        int i = 1;
        for (int index : list) {
            rawBytes[index] = inEncodedContent.charAt(i++);
        }
        
        return HCPUtils.fromBase64Encoding(new String(rawBytes));
    }
    
    public static void main(String[] args) {

        if (args.length == 1 && ! args[0].isEmpty()) {
            System.out.println(TextEncoder.encode(args[0]));
        }
        if (args.length == 2 && ! args[0].isEmpty() && "decode".equalsIgnoreCase(args[1])) {
            System.out.println(TextEncoder.decode(args[0]));
        }
    }

}
