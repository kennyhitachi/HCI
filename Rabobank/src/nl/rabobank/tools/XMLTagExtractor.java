package nl.rabobank.tools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class XMLTagExtractor {

	private static Options myOptions = new Options();
	
	private static void buildCommandEnvironment() {
		
		// Setup the options list
		myOptions.addOption("m", "multi-tag", false, "Indicates that the specified tag likely exists multiple times in the XML and thus should create a unique file for each tag found.");
		myOptions.addOption("n", "no-overwrite", false, "Do not over-write any existing files. Instead fail if file already exists.");
		myOptions.addOption("i", "id-tag", true, "Use the specified XML tag to create a unique identifier for the resultant file.");
		myOptions.addOption("o", "output-name", true, "Main name of resultant file(s). If '-' is specified, all output will go to the console. If not specfied will be the tag name. Do not specify file extension as .xml will be assigned.");
		myOptions.addOption("f", "format-output-xml", false, "Generate the resultant XML file(s) with formatting for easy human consumption.");
		myOptions.addOption("q", "quiet-mode", false, "Do not print out files that are generated.");
		myOptions.addOption("p", "processor", true, "Class name of processor to be called with each completed tag. Default is built-in disk file processor.");
		myOptions.addOption("x", "xargs", true, "Extended argument list to be passed to tag processor specified with -p option");
	}

	static final String CommandDesc 
	= "\nThis utility provides the ability to extract an XML tag and all its children and write those tags in separate file(s).\n"
	+ "The following are the command qualifiers: \n";

	static final String CommandArg 
	= "\nThe <tag-name> argument specifies the XML tag to extract from the input.\n"
	+ "The <input-xml-file> argument specifies the XML formatted file to extract tag(s) from. If this argument is not specified, the XML stream will be read from standard input.";

	// TODO Add information about xargs especially for the output-name argument so they know how to specify an alternative name.
	
	static void usage() {
		HelpFormatter myHelp = new HelpFormatter();
		
		myHelp.printHelp(
				"XMLTagExtractor <tag-name> [<input-xml-file>]",
				CommandDesc, 
				myOptions, 
				CommandArg);
	}
	
	/****
	 **** Member Variables.
	 ****/

	private String mTagName;
	private String mIdTagName;
	private InputStream mInputStream = System.in;
	private CommandLine mCommandLine;
	private XMLTagProcessorInterface mProcessor;

	/****
	 **** SAX Parser Events for generating tag files.
	 ****/
	private class SAXAbortedException extends SAXException {
		private static final long serialVersionUID = 1L;
		
		SAXAbortedException(Exception e) { super(e); }
		SAXAbortedException(String message, Exception e) { super(message, e); }
	}
	
	private class SAXDoneException extends SAXException {
		private static final long serialVersionUID = 1L;
		
		SAXDoneException(String message) { super(message); }
	}
	
	// This class processes the SAX events for an generic XML file.
	//  The core idea behind this event handler is create separate XML files
	//  with the tag and all its sub elements.
	private class SAXQueryEvents extends DefaultHandler {

		boolean bInTag = false;
		boolean bSawTag = false;
		String sIdentifierName;
		StringBuilder sCurrentValue;
		OutputStream mOutputStream;
		XMLStreamWriter mSerializer;
		int mDisplayLevel = 0;
		int mTagParseLevel = 0;
		String sPriorStart;
		
		void pushLevel() throws XMLStreamException {
			mDisplayLevel++;
		}
		
		void printNewLine() throws XMLStreamException {
			if (mCommandLine.hasOption('f')) {
				int level = mDisplayLevel;
				
				mSerializer.writeCharacters("\n");
				
				while (level > 0) {
					mSerializer.writeCharacters("  ");
					level--;
				}
				
			}
		}
		
		void popLevel() throws XMLStreamException {
			mDisplayLevel = Math.max(0, mDisplayLevel-1);
		}
		
		public void startElement(String namespaceURI, String localName, String qName, Attributes attrs) 
		   throws SAXException {

			// Did we already see the tag, 
			//    and we are back at the tag parse level of 0, 
			//    and we are not expecting multiples of the tag
			//  then we are done!
			if (bSawTag && mTagParseLevel == 0 && ! mCommandLine.hasOption('m') ) {
				throw new SAXDoneException("Already processed Tag");
			}
			
			// Did we find the tag we are looking for?
			if ( qName.equals(mTagName)) {
				bSawTag = true;
				
				// Create the file if this is the top most level we have seen the tag.
				if ( 0 == mTagParseLevel) {
					
					bInTag = true;
					
					try {
						mOutputStream = mProcessor.start();

						mSerializer = XMLOutputFactory.newInstance().createXMLStreamWriter(mOutputStream, "UTF-8");
						// TODO: BUG if -x "output-name=-" is provided, this is not handled here.
						// TODO:  Wonder if we should just drop the start document for everything.
						if ( ! mCommandLine.hasOption('o') ||  ! mCommandLine.getOptionValue('o').trim().equals("-")) {
							mSerializer.writeStartDocument("UTF-8", null);
							
							// If we aren't doing formatting, still print a newline after tag end.
							if ( ! mCommandLine.hasOption('f') )
								mSerializer.writeCharacters("\n");

						}
					} catch (Exception e) {
						throw new SAXAbortedException("Failed to start output processing.", e);
					}
				}
				
				mTagParseLevel++;
			}
			
			if (bInTag) {
				sPriorStart = qName;
				
				// Now let's put the information into the XML 

				try {
					printNewLine();
					mSerializer.writeStartElement(qName);
					pushLevel();

					// Write all the attributes.
					for (int idx = 0; idx < attrs.getLength(); idx++) {
						mSerializer.writeAttribute(attrs.getQName(idx), attrs.getValue(idx));
					}
				} catch (XMLStreamException e) {
					throw new SAXAbortedException(e);
				}
			}
			
			sCurrentValue=null;  // Don't need this.
		}
		
		public void endElement(String namespaceURI, String localName, String qName) {
			if (bInTag) {
				try {
					popLevel();
					if (null != sCurrentValue) {
						mSerializer.writeCharacters(sCurrentValue.toString());
					} else {
						if (! sPriorStart.equals(qName)) {
							// Must be a parent element.
							printNewLine();
						}
					}
					
					mSerializer.writeEndElement();
				} catch (XMLStreamException e1) {
					e1.printStackTrace();
					return;
				}

				// If we were given a tag id name, we don't already have one for this tag, 
				//   and we are just found the id tag, save it.
				if ( null != mIdTagName && null == sIdentifierName && qName.equals(mIdTagName)) {
					sIdentifierName = new String(sCurrentValue);
				}
			}
			
			if (qName.equals(mTagName)) {
				mTagParseLevel--;

				// If we reached the top most tag level, close out the file.
				if (0 == mTagParseLevel) {
					bInTag = false;
					try {
						printNewLine();
						popLevel();
						mSerializer.writeEndDocument();
						
						// If we aren't doing formatting, still print a newline after tag end.
						if ( ! mCommandLine.hasOption('f') )
							mSerializer.writeCharacters("\n");

						mProcessor.process(sIdentifierName);
					} catch (Exception e) {
						e.printStackTrace();
						return;
					}

					sIdentifierName = null;
				}
			}

			sCurrentValue = null;
		}
		
		// Function that receives tag value characters.
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (bInTag) {
				if (null == sCurrentValue) sCurrentValue = new StringBuilder();
				
				sCurrentValue.append(new String(ch, start, length));
			}
		}
	}
	
	/****
	 **** Core Class Member Functions.
	 ****/
	
	
	boolean initialize(String[] inArgs) {
		/*
		 * Parse Command Line
		 */
		try {
			mCommandLine = new BasicParser().parse(myOptions, inArgs);
		} catch (UnrecognizedOptionException e) {
			System.err.println("\nERROR: " + e.getMessage() + "\n");
			usage();
			
			return false;
		} catch (ParseException e) {
			System.err.println("\nERROR: Parsing Failure. " + e.getMessage() + "\n");
			usage();
			return false;
		}


		/*
		 * Validate Command Parameters/Qualifiers.
		 */
		String[] parsedArgs = mCommandLine.getArgs();

		// Must have at least one argument and it must be one of the value commands.
		if (parsedArgs == null || parsedArgs.length < 1 || parsedArgs.length > 2 ) {
			System.err.println("\nERROR: Invalid number of command parameters.\n");
			usage();
			
			return false;
		}
		
		mTagName = parsedArgs[0];
	
		// See if we have an input file or not.  If not, use standard Input stream for reading.
		if (parsedArgs.length == 2) {
			try {
				mInputStream = new FileInputStream(parsedArgs[1]);
				
			} catch (FileNotFoundException e1) {
				System.err.println("ERROR: <input-xml-file> does not exist:  " + parsedArgs[1]);
				
				return false;
			}
		}

		mIdTagName = mCommandLine.getOptionValue('i');

		String processorClass = mCommandLine.getOptionValue('p');
		if (null != processorClass) {
			// Make sure the class is in the class path.
			// Instantiate the object.
			try {
				mProcessor = (XMLTagProcessorInterface) Class.forName(processorClass).newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				System.err.printf("ERROR: Failed to construct object of name (%s).\n", processorClass);
				
				e.printStackTrace();
				return false;
			}
		} else {
			mProcessor = new XMLTagToFileProcessorImpl();
		}

		// Build the arguments to pass into the processor.
		StringBuilder xArgs = new StringBuilder();

		xArgs.append("tag-name="+mTagName);
		xArgs.append(",");
		
		String outputNameValue = mCommandLine.getOptionValue('o');
		if (null != outputNameValue) {
			xArgs.append("output-name="+outputNameValue);
			xArgs.append(",");
		}
		
		if (mCommandLine.hasOption('f')) {
			xArgs.append("format-xml=true");
			xArgs.append(",");
		}
		
		if (mCommandLine.hasOption('q')) {
			xArgs.append("quiet-mode=true");
			xArgs.append(",");
		}
		
		if (mCommandLine.hasOption('n')) {
			xArgs.append("overwrite-off=true");
			xArgs.append(",");
		}
		
		if (mCommandLine.hasOption('m')) {
			xArgs.append("multiple-files=true");
			xArgs.append(",");
		}
		
		xArgs.append(mCommandLine.getOptionValue('x'));
		
		// Pass in the string with any trailing ',' removed.
		return mProcessor.initialize(xArgs.toString().replaceAll(",$",  ""));
	}
	
	int run() {
		
		if ( ! mCommandLine.hasOption('q')) {
			System.out.println("Creating files for XML tag " + mTagName + ":");
			System.out.println();
		}
		
		/*
		 *  Now feed the response through the SAX Parser.
		 */
		
		XMLReader xmlReader;
		try {
			xmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			
			xmlReader.setContentHandler(new SAXQueryEvents());
			
			// Do the SAX Parsing.
			xmlReader.parse(new InputSource(mInputStream));
		} catch (SAXDoneException e) {
			// Normal exit.
		} catch (IOException | SAXException | ParserConfigurationException e) {
			// TODO
			// Unexpected parsing failure.
			e.printStackTrace();
			
			return 1;
		} finally {
			// Cleanup Time.
			if (null != mInputStream) {
				try {
					mInputStream.close();
				} catch (IOException e) {
					// best try.  Don't really care.
				}
			}
		}
		
		return 0;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int retval = 0; // Be optimistic.
		
		/*
		 * Setup
		 */
		buildCommandEnvironment();

		XMLTagExtractor me = new XMLTagExtractor();

		if (me.initialize(args)) {
			retval = me.run();  // Let's do it.
		} else {
			retval = 1;  // Didn't initialize properly.
		}
		
		// All done.
		System.exit(retval);
	}

}
