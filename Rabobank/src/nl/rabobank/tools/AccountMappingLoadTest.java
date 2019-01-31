package nl.rabobank.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class AccountMappingLoadTest {

	HashMap<Integer, String> mAccountRegionMap = new HashMap<Integer, String>();
	
	void loadAccountRegionMap() throws IOException {
		File mappingFile = new File("AccountMapping.lst");
		
		BufferedReader br = new BufferedReader(new FileReader(mappingFile));
		
		String line;
		while ((line = br.readLine()) != null) {
			
			// Remove any possible ending line comments
			String rawParts[] = line.split("#");
			
			if (rawParts.length < 1) {
				continue;  // junk line
			}

			// Get the parts of the meat of the line and remove extra spaces and such.
			String parts[] = rawParts[0].trim().replaceAll("\\s+",  " ").split(" ");
			
			if (parts.length >= 2) {
				String regionCode = parts[0];
				// Process all account(s) on the line
				for (int i = 1; i < parts.length; i++) {
					Integer accountNumber = new Integer(parts[i]);
					
					System.out.printf("%s[%d]: %s\n", regionCode, i, parts[i]);

					if (0 != accountNumber) {
						mAccountRegionMap.put(accountNumber, regionCode); // Latest always wins.
					}
				}
			}
		}
		
		br.close();
	}
	
	String getRegion(Integer inAccount) {
		return mAccountRegionMap.get(inAccount);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		AccountMappingLoadTest me = new AccountMappingLoadTest();
		
		try {
			me.loadAccountRegionMap();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("getRegion: " + me.getRegion(196970));
		
		// TODO Auto-generated method stub

	}

}
