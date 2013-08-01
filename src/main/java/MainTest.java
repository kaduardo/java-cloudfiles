import java.io.IOException;
import java.security.KeyStore;
import java.util.LinkedList;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;

import com.rackspacecloud.client.cloudfiles.FilesClientFederatedKeystone;


public class MainTest {
	
	private static final String USERNAME = "funcionario";
	private static final String PASSWORD = "funcionario123";
	private static final String AUTH_URL = "https://pinga.ect.ufrn.br:5000/v2.0";
	private static final String ACCOUNT = "";
	private static final String REALM = "IdP+ STCFED";
	private static final String REALM_ENDPOINT = "http://idpstcfed.sj.ifsc.edu.br/RNPSecurityTokenService/RNPSTS";
	private static final String TENANT = "Idp+ Personal User's Project";
	
	public static void main(String[] args) throws IOException, Exception {
		
		HttpClient federatedKeystoneclient = getNewHttpClient();
		FilesClientFederatedKeystone federatedClient = new FilesClientFederatedKeystone(federatedKeystoneclient, USERNAME, PASSWORD, AUTH_URL, ACCOUNT, 0);
		/*  ---- GETREALMLIST() ----*/
		LinkedList<String> realms = (LinkedList<String>) federatedClient.getRealmList();
		System.out.println("IdP's disponíveis: ");
		for (String realm : realms) {
			System.out.println(realm);
		}
		
		
		String[] idpRequest = federatedClient.getIdPRequest(REALM);
		
		/* ---- GETIDPRESPONSE ----
		 * 
		 * O endpoint que está no request é diferente do que realmente deveria ser.
		 * 
		 * */
		String idpResponse = federatedClient.getIdPResponse(REALM_ENDPOINT, idpRequest[1]);
		
		System.out.println(idpResponse);
		
		String unscopedTokenJson = federatedClient.getUnscopedToken(idpResponse, REALM);
		System.out.println(unscopedTokenJson);
		
		federatedClient.getScopedToken(federatedClient.getUnscopedToken(), TENANT);
		
	}
	
	public static HttpClient getNewHttpClient() {
	    try {
	        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
	        trustStore.load(null, null);

	        SSLSocketFactory sf = new MySSLSocketFactory(trustStore);
	        sf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

	        HttpParams params = new BasicHttpParams();
	        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
	        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);

	        SchemeRegistry registry = new SchemeRegistry();
	        registry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
	        registry.register(new Scheme("https", sf, 443));

	        ClientConnectionManager ccm = new ThreadSafeClientConnManager(params, registry);

	        return new DefaultHttpClient(ccm, params);
	    } catch (Exception e) {
	        return new DefaultHttpClient();
	    }
	}

}

