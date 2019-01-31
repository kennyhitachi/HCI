package nl.rabobank.comet.generator.utils;

import java.io.InvalidClassException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * Abstract class to act as a container for custom metadata extracted from Bloomberg content.
 *   The Metadata collected is based on a specific field name and makes assumptions about the
 *   XML structure being Bloomberg.
 *   This class provides base functionality for adding a new metadata value, updating existing
 *   values by incrementing, max and min value methods.
 *   
 * @author cgrimm
 *
 */
public abstract class BloombergMetadataBaseContainer {
	
	BloombergMetadataBaseContainer(String inFieldName, SimpleDateFormat inOutputFormatter) {
		mFieldName = inFieldName;
		mOutputMetadataDateFormat = inOutputFormatter;
	}
	
	// Common Top level metadata names.
	public static final String START_TIME_ID = "StartTimeUTC";
	public static final String END_TIME_ID = "EndTimeUTC";
	public static final String TIME_ID = "DateTimeUTC";
	public static final String COUNT_ID = "Count";
	
	protected String mFieldName;
	protected SimpleDateFormat mOutputMetadataDateFormat;
	protected HashMap<String, Object> mMetadataMap = new HashMap<String, Object>();
	
	public abstract String generateXML(String inTrailingMetadata);

	public String getFieldName() { return mFieldName; }
	
	public void setItem(String inName, Object inValue) throws InvalidClassException {
		Object value = mMetadataMap.get(inName);
		
		if (null != value && ! inValue.getClass().equals(value.getClass())) {
			throw new InvalidClassException("Input parameter does not match previously stored value");
		}
		
		if ( ! ( inValue instanceof String || inValue instanceof Long || inValue instanceof Integer ) ) {
			throw new IllegalArgumentException("Unexpected data type provided as input parameter");
		}

		mMetadataMap.put(inName, inValue);
	}

	public void incrementItem(String inName)
			throws IllegalArgumentException {
		Object value = mMetadataMap.get(inName);

		if ( null != value && ! (value instanceof Integer || value instanceof Long) ) {
			throw new IllegalArgumentException("Argument must reference either an Integer or Long");
		}
		
		if (null == value ) {
			mMetadataMap.put(inName, new Long(1));
		} else {
			Object newValue = null;
			if (value instanceof Integer) {
				newValue = ((Integer)value) + 1;
			} else {
				newValue = ((Long)value) + 1;
			}
			
			mMetadataMap.put(inName, newValue);
		}
	}

	public void maxItem(String inName, Object inValue)
			throws IllegalArgumentException {
		Object value = mMetadataMap.get(inName);

		if ( null != value && ! inValue.getClass().equals(value.getClass())) {
			throw new IllegalArgumentException("Input value data-type does not match previous stored value");
		}
		
		if ( ! ( inValue instanceof String || inValue instanceof Long || inValue instanceof Integer ) ) {
			throw new IllegalArgumentException("Unexpected data type provided as input parameter");
		}

		if (null == value ) {
			mMetadataMap.put(inName, inValue);
		} else {
			if ( value instanceof String ) {
				String input = (String)inValue;
				String existing = (String)value;
				
				if (0 > input.compareTo(existing)){
					mMetadataMap.put(inName, input);
				}
			} else if (value instanceof Long) {
				Long input = (Long)inValue;
				Long existing = (Long)value;
				
				if (input > existing) {
					mMetadataMap.put(inName, input);
				}
			} else if (value instanceof Integer) {
				Integer input = (Integer)inValue;
				Integer existing = (Integer)value;
				
				if (input > existing) {
					mMetadataMap.put(inName, input);
				}
			}
		}
	}

	public void minItem(String inName, Object inValue)
			throws InvalidClassException {
		Object value = mMetadataMap.get(inName);

		if ( null != value && ! inValue.getClass().equals(value.getClass())) {
			throw new IllegalArgumentException("Input value data-type does not match previous stored value");
		}
		
		if ( ! ( inValue instanceof String || inValue instanceof Long || inValue instanceof Integer ) ) {
			throw new IllegalArgumentException("Unexpected data type provided as input parameter");
		}

		if ( null == value ) {
			// If no previous value, start with this one.
			mMetadataMap.put(inName, inValue);
		} else {
			
			if ( value instanceof String ) {
				String input = (String)inValue;
				String existing = (String)value;
				
				if (0 < input.compareTo(existing)){
					mMetadataMap.put(inName, input);
				}
			} else if (value instanceof Long) {
				Long input = (Long)inValue;
				Long existing = (Long)value;
				
				if (input < existing) {
					mMetadataMap.put(inName, input);
				}
			} else if (value instanceof Integer) {
				Integer input = (Integer)inValue;
				Integer existing = (Integer)value;
				
				if (input < existing) {
					mMetadataMap.put(inName, input);
				}
			}
		}
	}
}
