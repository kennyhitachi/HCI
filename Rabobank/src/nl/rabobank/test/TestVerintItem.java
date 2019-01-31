package nl.rabobank.test;

import java.io.File;

import com.hds.hcp.tools.comet.FileSystemItem;

import nl.rabobank.comet.VerintItem;

public class TestVerintItem {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		File oneFolder = new File("/home/cgrimm/Customers/Rabobank/SearchConsole/Data/Ingest/HCP_Production/Verint/NL_data/Avaya Export;1_20150227_124300/WAV");
		
		File files[] = oneFolder.listFiles();

		for (int i = 0; i < files.length; i++) {
			VerintItem oneItem = new VerintItem(files[i], null);

			if (oneItem instanceof FileSystemItem) {
				if (oneItem.exists()) {
					oneItem.delete();
				}
			}
		}
		
		// TODO Auto-generated method stub
		
		
		
	}

}
