package nl.rabobank.comet.generator.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BloombergConversationMetadata extends BloombergMetadataBaseContainer {

	public BloombergConversationMetadata(SimpleDateFormat inOutputFormatter) {
		// Bloomberg Conversation keys off of DateTimeUTC tag for metadata collection
		super("DateTimeUTC", inOutputFormatter);
	}

	// Bloomberg specific fields that containe the "DateTimeUTC" value for which we should collect.
	public static final String ATTACHMENT_ID = "Attachment";
	public static final String INVITE_ID = "Invite";
	public static final String PARTICIPANT_ENTERED_ID = "ParticipantEntered";
	public static final String PARTICIPANT_LEFT_ID = "ParticipantLeft";
	public static final String MESSAGE_ID = "Message";
	public static final String HISTORY_ID = "History";
	public static final String SYSTEM_MESSAGE_ID = "SystemMessage";
	
	// General high level information that will be collected.
	public static final String ROOM_ID = "RoomID";

	//  Array of activities to aid with processing in a general fashion.
	private String mActivities[] = { ATTACHMENT_ID, INVITE_ID, PARTICIPANT_ENTERED_ID, PARTICIPANT_LEFT_ID, MESSAGE_ID, HISTORY_ID, SYSTEM_MESSAGE_ID };

	// Create the Bloomberg Conversation specfic metadata based on existance of the collected metadata.
	public String generateXML(String inTrailingMetadata) {
		StringBuilder retval = new StringBuilder();
		
		retval.append("  <Conversation>\n");
		String value = (String)mMetadataMap.get(ROOM_ID);
		if (null != value) {
			retval.append("    <RoomID>" + value + "</RoomID>\n");
		}
		
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
		
		Long count = (Long)mMetadataMap.get(COUNT_ID);
		if (null == count) {
			count = (long)0;
		}
		
		retval.append("    <Activities count=\"" + count + "\">\n");

		if ( 0 != count) {
			// Loop through all the possible activities in the Conversation to and construct metadata based
			//   on what exists.
			for (int i = 0; i < mActivities.length; i++) {
				
				count = (Long)mMetadataMap.get(mActivities[i] + "_" + COUNT_ID);
				if (null != count) {
					// Only build out an activity if the count for the activity exists.
					retval.append("      <" + mActivities[i] + " count=\"" + count + "\">\n");
					
					if (mMetadataMap.containsKey(mActivities[i] + "_" + START_TIME_ID)) {
						retval.append("        <StartDateTime>" + 
							     mOutputMetadataDateFormat.format(new Date((Long)mMetadataMap.get(mActivities[i] + "_" + START_TIME_ID) * 1000))
							     + "</StartDateTime>\n");
					}
					if (mMetadataMap.containsKey(mActivities[i] + "_" + END_TIME_ID)) {
						retval.append("        <EndDateTime>" + 
							     mOutputMetadataDateFormat.format(new Date((Long)mMetadataMap.get(mActivities[i] + "_" + END_TIME_ID) * 1000))
							     + "</EndDateTime>\n");
					}
					
					retval.append("      </" + mActivities[i] + ">\n");
				}
			}
		}
		
		retval.append("    </Activities>\n");
		
		if (null != inTrailingMetadata) {
			retval.append(inTrailingMetadata);
		}

		retval.append("  </Conversation>\n");
		
		return retval.toString();
	}
}
