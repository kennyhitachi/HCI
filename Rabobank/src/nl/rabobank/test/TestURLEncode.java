package nl.rabobank.test;

import java.net.URISyntaxException;

import com.hds.hcp.tools.comet.utils.URIWrapper;

public class TestURLEncode {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String unencodedURI = "https://ns1.ten1.clghcp700.local/rest/Reuters/2013-03-26T1934+0000/Todays Folder #1/GUID{349024AJKLGF-dkf}?type=custom-metadata";

		try {
			String encodedURI;
			encodedURI = URIWrapper.encode(unencodedURI);
			
			
			encodedURI = URIWrapper.encode("#{");
			System.out.println(encodedURI);
			encodedURI = URIWrapper.encode("Hello");
			encodedURI = URIWrapper.encode("Hello There ");
			encodedURI = URIWrapper.encode(" Hello There ");
			encodedURI = URIWrapper.encode("/hello/there");
			encodedURI = URIWrapper.encode("/Hello/#");

			encodedURI = URIWrapper.encode(unencodedURI);

			URIWrapper uriwrapper = null;
			try {
				uriwrapper = new URIWrapper("jfdsajk");
			} catch (URISyntaxException e) {
				System.out.println("Negative Case pass");
			}
			
			try {
				uriwrapper = new URIWrapper("https:/");
			} catch (URISyntaxException e) {
				System.out.println("Negative Case pass");
			}
			try {
				uriwrapper = new URIWrapper("https://");
			} catch (URISyntaxException e) {
				System.out.println("Negative Case pass");
			}
			uriwrapper = new URIWrapper("https://my.host.local");
			System.out.println(uriwrapper.toString());
			uriwrapper = new URIWrapper("https://my.host.local/");
			System.out.println(uriwrapper.toString());
			uriwrapper = new URIWrapper("https://my.host.local/path1");
			System.out.println(uriwrapper.toString());
			try {
				uriwrapper = new URIWrapper("https://my.host.local/path1?");
			} catch (URISyntaxException e) {
				System.out.println("Negative Case pass");
			}
			uriwrapper = new URIWrapper("https://my.host.local/path1?type=custom-metadata");
			System.out.println(uriwrapper.toString());

			uriwrapper = new URIWrapper(unencodedURI);
			System.out.println(uriwrapper.toString());
			System.out.println(uriwrapper.toRawString());
			
			uriwrapper.setPath(URIWrapper.encode(uriwrapper.getRawPath()));
			System.out.println(uriwrapper.toString());

			System.out.println(uriwrapper.toPathOnlyURIWrapper().toString());
			
			

			/*
			URL url = new URL(unencodedURI);
			System.out.println(url);
			url = new URL(encodedURI);
			System.out.println(url);
			System.out.println(new URL(url.getProtocol(), url.getHost(), url.getFile()));
			System.out.println(new URL(url.getProtocol(), url.getHost(), url.getPath()));

			URI uri = null;
//			uri = new URI(unencodedURI);  Throws an exception.
			System.out.println(uri);
			uri = new URI(encodedURI + "?type=custom-metadata");
			System.out.println(uri); // This is all encoded.
			System.out.println(uri.toString()); // This is all encoded.
			System.out.println(uri.getPath()); // This returns string all unencoded.
			System.out.println(uri.getRawPath()); // This returns string all unencoded.
			URI pathOnly = new URI(uri.getScheme(), uri.getHost(), uri.getRawPath(), null);
			System.out.println(pathOnly.toString());
			pathOnly = new URI(uri.getScheme(), uri.getHost(), URIWrapper.encode(uri.getPath()), null);
			System.out.println(pathOnly.toString());
			
			System.out.println(URIWrapper.encode(uri.getPath())); // This returns string all unencoded.
			URI tmp = new URI(uri.getScheme(), uri.getHost(), URIWrapper.encode(uri.getPath()));
			System.out.println(tmp.toString());
			System.out.println(tmp.getPath());
			
			System.out.println(new URI(uri.getScheme(), uri.getHost(), uri.getPath()).toString()); // This returns encoded, but the '+' sign.
			System.out.println(new URI(uri.getScheme(), uri.getHost(), URIWrapper.encode(uri.getPath())));

			*/
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
//			val = URLEncoder.encode("https://ns1.ten1.clghcp700.local/rest/Reuters/2013-03-26T1934+0000/Todays File", "UTF-8");
			
//			val = URLEncoder.encode("https://ns1/rest/Foo{bar}Today is", "UTF-8");
		
	}

}
