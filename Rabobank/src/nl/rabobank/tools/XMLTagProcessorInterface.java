package nl.rabobank.tools;

import java.io.OutputStream;

public interface XMLTagProcessorInterface {
	
boolean initialize(String inParams);
OutputStream start() throws Exception;
void process(String inTagId) throws Exception;
void close();
}
