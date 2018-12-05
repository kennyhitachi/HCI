#!/bin/bash

LIB=`pwd -P`/libs

if java -version 2>&1 >/dev/null | grep -q "java version" ; then
 

#   Define the class path entries for the content class modules that will
#   assist in processing. This line must be modified in order to pick up
#   the java entry points to these modules.
CUSTOM_MODULES=${LIB}/ContentClass.jar

CLASSPATH=${LIB}/httpcore-4.2.4.jar:${LIB}/httpclient-4.2.5.jar:${LIB}/commons-codec-1.10.jar:${LIB}/commons-logging-1.1.1.jar
CLASSPATH=${CLASSPATH}:${LIB}/commons-collections4-4.1.jar:${LIB}/commons-cli-1.4.jar
CLASSPATH=${CLASSPATH}:${LIB}/commons-lang-2.5.jar:${LIB}/commons-io-2.5.jar
CLASSPATH=${CLASSPATH}:${LIB}/jackson-annotations-2.5.1.jar:${LIB}/jackson-all-1.9.11.jar
CLASSPATH=${CLASSPATH}:${LIB}/jackson-databind-2.5.1.jar:${LIB}/jackson-core-2.5.1.jar
CLASSPATH=${CLASSPATH}:${LIB}/poi-3.17.jar:${LIB}/jackson-dataformat-csv-2.5.1.jar
CLASSPATH=${CLASSPATH}:${LIB}/poi-ooxml-3.17.jar:${LIB}/poi-ooxml-schemas-3.17.jar
CLASSPATH=${CLASSPATH}:${LIB}/xmlbeans-2.6.0.jar
CLASSPATH=${CLASSPATH}:${CUSTOM_MODULES}
export CLASSPATH

java com.hitachi.hci.content.loader.ContentClassMain $*

else

echo "Unable to find JAVA. Please install JAVA 1.8 or higher and run the script."

fi

exit 0

