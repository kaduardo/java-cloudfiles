package com.rackspacecloud.client.cloudfiles;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.http.HttpException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilesClientKeystoneTestCase extends TestCase {

	private String KEYSTONE_URL = "https://pinga.ect.ufrn.br:5000/v2.0/tokens";
	
	private String TENANT = "gttenant";
	private String USERNAME = "gtuser";
	private String PASSWORD = "gtpwd";
	
	private static File SYSTEM_TMP = SystemUtils.getJavaIoTmpDir();
	private static int NUMBER_RANDOM_BYTES = 513;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testLogin() {
		
		FilesClientKeystone client = 
		new FilesClientKeystone(new MyHttpClientTrustAll(),
				USERNAME + ":" + TENANT, 
				PASSWORD, 
				KEYSTONE_URL, 
				null, 10000);		
		try {
			System.out.println("Login: " + client.login());
			
			System.out.println(client.getAuthenticationURL());
			System.out.println(client.getAuthToken());
			System.out.println(client.getStorageURL());
			
		} catch (IOException | HttpException e) {

			e.printStackTrace();
		}
		try {
			assertTrue(client.login());
		} catch (Exception e) {
			fail(e.getMessage());
		} 
		
		fail("Not yet implemented"); // TODO
	}

	@Test
	public void testAuthenticate() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public void testLoginStringStringString() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public void testListContainersInfo() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public void testListContainers() {
		String containerName = createTempContainerName("<container>");
		String filename = makeFileName("<object>");
		String fullPath = FilenameUtils.concat(SYSTEM_TMP.getAbsolutePath(), filename);
		
		try {
			byte randomData[] = makeRandomFile(fullPath);
			FilesClientKeystone client = 
					new FilesClientKeystone(new MyHttpClientTrustAll(),
							USERNAME + ":" + TENANT, 
							PASSWORD, 
							KEYSTONE_URL, 
							null, 10000);
			
			assertTrue(client.login());
			
			// Set up
			client.createContainer(containerName);
			
			// Store it
			System.out.println("About to save: " + filename);
			assertNotNull(client.storeObjectAs(containerName, new File(fullPath), "application/octet-stream", filename));
			
			// Make sure it's there
			List<FilesObject> objects = client.listObjects(containerName);
			assertEquals(1, objects.size());
			FilesObject obj = objects.get(0);
			assertEquals(filename, obj.getName());
			assertEquals("application/octet-stream", obj.getMimeType());
			assertEquals(NUMBER_RANDOM_BYTES, obj.getSize());
			assertEquals(md5Sum(randomData), obj.getMd5sum());
			
			// Make sure the data is correct
			assertArrayEquals(randomData, client.getObject(containerName, filename));
			
			// Make sure the data is correct as a stream
			InputStream is = client.getObjectAsStream(containerName, filename);
			byte otherData[] = new byte[NUMBER_RANDOM_BYTES];
			is.read(otherData);
			assertArrayEquals(randomData, otherData);
			assertEquals(-1, is.read()); // Could hang if there's a bug on the other end
			
			// Clean up 
			client.deleteObject(containerName, filename);
			assertTrue(client.deleteContainer(containerName));
			
		}
		catch (Exception e) {
			fail(e.getMessage());
		}
		finally {
			File f = new File(fullPath);
			f.delete();
		}
	}

	
	//Utilities methods
	
	private String createTempContainerName(String addition) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS");
		return "test-container-" + addition + "-" + sdf.format(new Date(System.currentTimeMillis()));
	}
	
	private String makeFileName(String addition) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS");
		return "test-file-" + addition + "-" + sdf.format(new Date(System.currentTimeMillis()));
	}
	
	private byte[] makeRandomFile(String name) throws IOException {

		File file = new File(name);
		FileOutputStream fos = new FileOutputStream(file);
		byte randomData[] = makeRandomBytes();
		fos.write(randomData);
		fos.close();
		
		return randomData;
	}
	
	private byte[] makeRandomBytes() {
		return makeRandomBytes(NUMBER_RANDOM_BYTES);
	}
	private byte[] makeRandomBytes(int nBytes) {
		byte results[] = new byte[nBytes];
		Random gen = new Random();
		gen.nextBytes(results);
		
		// Uncomment to get some not so random data
		// for(int i=0; i < results.length; ++i) results[i] = (byte) (i % Byte.MAX_VALUE);
		
		return results;
	}
	   private static String md5Sum (byte[] data) throws IOException, NoSuchAlgorithmException
	    {
	    	MessageDigest digest = MessageDigest.getInstance("MD5");

	    	byte[] md5sum = digest.digest(data);
	    	BigInteger bigInt = new BigInteger(1, md5sum);
	    	
	    	String result = bigInt.toString(16);
	    	
	    	while(result.length() < 32) {
	    		result = "0" + result;
	    	}

	    	return result;
	    }
	
	private void assertArrayEquals(byte a[], byte b[]) {
		assertEquals(a.length, b.length);
		for(int i=0; i < a.length; ++i) assertEquals(a[i], b[i]);
	}
}
