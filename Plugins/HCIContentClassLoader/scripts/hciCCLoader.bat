@echo off

where java >nul 2>nul
if %errorlevel%==1 (
    @echo Unable to find JAVA. Please install JAVA 1.8 or higher and run the script.
    exit
)

set TOOL_HOME=%cd%
set TOOL_LIBS=%TOOL_HOME%\libs


REM   Define the class path entries for the content class modules that will
REM   assist in processing. This line must be modified in order to pick up
REM   the java entry points to these modules.
set CUSTOM_MODULES=%TOOL_LIBS%\ContentClass.jar

set CLASSPATH=%TOOL_LIBS%\httpcore-4.2.4.jar;%TOOL_LIBS%\httpclient-4.2.5.jar;%TOOL_LIBS%\commons-codec-1.10.jar;%TOOL_LIBS%\commons-logging-1.1.1.jar
set CLASSPATH=%CLASSPATH%;%TOOL_LIBS%\commons-collections4-4.1.jar;%TOOL_LIBS%\commons-cli-1.4.jar
set CLASSPATH=%CLASSPATH%;%TOOL_LIBS%\commons-lang-2.5.jar;%TOOL_LIBS%\commons-io-2.5.jar
set CLASSPATH=%CLASSPATH%;%TOOL_LIBS%\jackson-annotations-2.5.1.jar;%TOOL_LIBS%\jackson-all-1.9.11.jar
set CLASSPATH=%CLASSPATH%;%TOOL_LIBS%\jackson-databind-2.5.1.jar;%TOOL_LIBS%\jackson-core-2.5.1.jar
set CLASSPATH=%CLASSPATH%;%TOOL_LIBS%\poi-3.17.jar;%TOOL_LIBS%\jackson-dataformat-csv-2.5.1.jar
set CLASSPATH=%CLASSPATH%;%TOOL_LIBS%\poi-ooxml-3.17.jar;%TOOL_LIBS%\poi-ooxml-schemas-3.17.jar
set CLASSPATH=%CLASSPATH%;%TOOL_LIBS%\xmlbeans-2.6.0.jar
set CLASSPATH=%CLASSPATH%;%CUSTOM_MODULES%


java com.hitachi.hci.content.loader.ContentClassMain %*
