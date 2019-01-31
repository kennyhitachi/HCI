package nl.rabobank.test;

import nl.rabobank.comet.util.InvalidConfigurationException;
import nl.rabobank.comet.util.RegionMapperProperties;
import nl.rabobank.comet.util.RegionSpec;

public class TestRegionMapper {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		RegionMapperProperties myMapper = new RegionMapperProperties();
		
		try {
			RegionSpec oneSpec = null;
			
			oneSpec = myMapper.getMatch("default");
			System.out.println(oneSpec);
			
			oneSpec = myMapper.getMatch("usa");
			System.out.println(oneSpec);
			
			oneSpec = myMapper.getMatch("uk");
			System.out.println(oneSpec);
			
			oneSpec = myMapper.getMatch("bad");
			if (null != oneSpec)
				System.out.println("Humm.. Not working");
		} catch (InvalidConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
