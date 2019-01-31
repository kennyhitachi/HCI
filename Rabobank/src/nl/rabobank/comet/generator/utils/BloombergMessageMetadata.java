package nl.rabobank.comet.generator.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BloombergMessageMetadata extends BloombergMetadataBaseContainer {

	public BloombergMessageMetadata(SimpleDateFormat inOutputFormatter) {
		// Bloomberg Messages keys off of MsgTimeUTC tag for metadata collection
		super("MsgTimeUTC", inOutputFormatter);
	}

	// Create the Bloomberg Message metadata form based on what was collected.
	public String generateXML(String inTrailingMetadata) {
		StringBuilder retval = new StringBuilder();
		
		Long count = (Long)mMetadataMap.get(COUNT_ID);
		if (null == count) {
			count = (long)0;
		}
		retval.append("  <Messages count=\"" + count + "\">\n");
		
		if (mMetadataMap.containsKey(START_TIME_ID)) {
			retval.append("    <StartDateTime>" + 
				     mOutputMetadataDateFormat.format(new Date((Long)mMetadataMap.get(START_TIME_ID) * 1000))
				     + "</StartDateTime>\n");
		}
		if (mMetadataMap.containsKey(END_TIME_ID)) {
			retval.append("    <EndDateTime>" + 
				     mOutputMetadataDateFormat.format(new Date((Long)mMetadataMap.get(END_TIME_ID) * 1000))
				     + "</EndDateTime>\n");
		}
		
		if (null != inTrailingMetadata ) {
			retval.append(inTrailingMetadata);
		}

		retval.append("  </Messages>\n");
		
		return retval.toString();
	}
}
