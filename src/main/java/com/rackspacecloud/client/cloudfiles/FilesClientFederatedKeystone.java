/*
 * See COPYING for license information.
 */

package com.rackspacecloud.client.cloudfiles;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import br.rnp.stcfed.sts.client.impl.IDPMaisClient;

import com.sun.xml.ws.api.security.trust.WSTrustException;

public class FilesClientFederatedKeystone extends FilesClientKeystone {
	private static Logger logger = Logger
			.getLogger(FilesClientFederatedKeystone.class);

	/**
	 * @param client
	 *            The HttpClient to talk to Swift
	 * @param username
	 *            The username to log in to
	 * @param password
	 *            The password
	 * @param authUrl
	 *            Authentication URL
	 * @param account
	 *            The Cloud Files account to use
	 * @param connectionTimeOut
	 *            The connection timeout, in ms.
	 */
	public FilesClientFederatedKeystone(HttpClient client, String username,
			String password, String authUrl, String account,
			int connectionTimeOut) {
		super(client, username, password, authUrl, account, connectionTimeOut);
	}

	/**
	 * @param username
	 *            The username
	 * @param password
	 *            The API key
	 * @param authUrl
	 *            Authentication URL
	 * @param account
	 *            The Cloud Files account to use
	 * @param connectionTimeOut
	 *            The connection timeout, in ms.
	 */
	public FilesClientFederatedKeystone(String username, String password,
			String authUrl, String account, final int connectionTimeOut) {
		super(username, password, authUrl, account, connectionTimeOut);
	}

	@Override
	public boolean login() throws IOException, HttpException {
		throw new IllegalStateException("This method can not be called in a federated authentication.");
	}
	
	@Override
	public void authenticate()  throws IOException, HttpException {
		throw new IllegalStateException("This method can not be called in a federated authentication.");
	}

	/**
	 * Creates a POST request to the Service Provide (federated keystone),
	 * requesting the IDP list from the authenticationURL informed in the
	 * constructor
	 * 
	 * @param spEndpoint
	 * @return
	 */
	public List<String> getRealmList() throws IOException, Exception {

		return this.getRealmList(getAuthenticationURL());
	}

	/**
	 * Creates a POST request to the Service Provide (federated keystone),
	 * requesting the IDP list
	 * 
	 * @param spEndpoint
	 * @return
	 */
	public List<String> getRealmList(String spEndpoint)
			throws IOException, Exception {
		HttpPost httppost = new HttpPost(spEndpoint);

		try {

			// cria json sem conteudo e o insere no corpo (body) da requisicao
			StringEntity entity = new StringEntity("{}");

			entity.setContentType("application/json");
			httppost.setEntity(entity);
			httppost.addHeader("Content-type", "application/json");
			httppost.addHeader("X-Authentication-Type", "federated");

			logger.debug("request: " + httppost.toString());

			// vai tratar a resposta da requisição
			HttpResponse resp = client.execute(httppost);

			// transforma resposta em uma string contendo o json da resposta
			String response = httpEntityToString(resp.getEntity());

			JSONObject jsonResp = new JSONObject(response);

			// OBS.: realm=IDP
			JSONArray realms = jsonResp.getJSONArray("realms");

			LinkedList<String> idps = new LinkedList<String>();

			for (int i = 0; i < realms.length(); ++i) {
				JSONObject realm = realms.getJSONObject(i);
				idps.add(realm.getString("name"));
				// System.out.println("realm: "+realm.getString("name"));
			}
			return idps;
		} finally {
			httppost.abort();
		}
	}

	
	/**
	 * Requests the SP a IDP's endpoint and the SAML needed to send to it in
	 * order to authentication on the IDP
	 * 
	 * @param idpName
	 * @return
	 * @throws IOException
	 * @throws ClientProtocolException
	 * @throws JSONException
	 */
	public String[] getIdPRequest(String realm)
			throws ClientProtocolException, IOException, JSONException {

		return this.getIdPRequest(getAuthenticationURL(), realm);
	}
	
	/**
	 * Requests the SP a IDP's endpoint and the SAML needed to send to it in
	 * order to authentication on the IDP
	 * 
	 * @param idpName
	 * @return
	 * @throws IOException
	 * @throws ClientProtocolException
	 * @throws JSONException
	 */
	public String[] getIdPRequest(String spEndpoint, String realm)
			throws ClientProtocolException, IOException, JSONException {
		String[] responses = new String[2];
		HttpPost httppost = new HttpPost(spEndpoint);

		try {

			// cria json com crenciais para requisitar autenticação
			StringEntity entity = new StringEntity("{\"realm\": {\"name\":\""
					+ realm + "\"}}");

			entity.setContentType("application/json");
			httppost.setEntity(entity);
			httppost.addHeader("Content-type", "application/json");
			httppost.addHeader("X-Authentication-Type", "federated");

			// vai tratar a resposta da requisição
			HttpResponse requestResp = client.execute(httppost);

			// transforma resposta em uma string contendo o json da resposta
			String responseAsString = httpEntityToString(requestResp
					.getEntity());

			JSONObject jsonResp = new JSONObject(responseAsString);

			responses[0] = jsonResp.getString("idpEndpoint");
			responses[1] = jsonResp.getString("idpRequest");

			logger.debug("idpEndpoint:\n" + responses[0]);

			logger.debug("idpRequest:\n" + responses[1]);

			return responses;
		} finally {
			httppost.abort();
		}

	}


	
	public String getIdPResponse(String idpEndpoint, String samlRequest)
			throws WSTrustException, UnsupportedEncodingException,
			DataFormatException, DecoderException {

		String entityID = getEntityID(samlRequest);

		logger.debug("\n entity ID: " + entityID);

		IDPMaisClient idpMais = new IDPMaisClient();

		// alterar para idpEndpoint quando este for atualizado (?)
		String idpResponse = idpMais
				.getIDPMaisSAMLResponse(
						getUserName(),
						getPassword(),
						idpEndpoint,
						entityID);

		return idpResponse;
	}

	/**
	 * Returns a Json containing the unscoped token and a JsonArray of the
	 * user's tenants
	 * 
	 * @param idpResponse
	 * @param idpName
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws JSONException
	 */
	public String getUnscopedToken(String idpResponse, String idpName)
			throws UnsupportedEncodingException, JSONException {

		HttpPost postRequest = new HttpPost(getAuthenticationURL());

		StringEntity entity = new StringEntity("{\"realm\":{\"name\":\""
				+ idpName + "\"},\"idpResponse\":\"SAMLResponse="
				+ URLEncoder.encode(idpResponse, "UTF-8") + "\"}");

		postRequest.setEntity(entity);

		entity.setContentType("application/json");
		postRequest.setHeader("Content-Type", "application/json");
		postRequest.setHeader("X-Authentication-Type", "federated");

		HttpResponse response;

		try {
			response = client.execute(postRequest);
			String unscopedTokenJson = httpEntityToString(response.getEntity());

			return unscopedTokenJson;
		} catch (IOException e) {
			e.printStackTrace();

			return "error";
		}

	}

	public String getScopedToken(String unscopedTokenJson, String tenantFn)
			throws JSONException, UnsupportedEncodingException {
		unscopedTokenJson.trim();
		JSONObject SPResponse = new JSONObject(unscopedTokenJson);
		String unscopedToken = SPResponse.getString("unscopedToken");
		logger.debug("unscopedToken: " + unscopedToken);

		String tenantId = "";
		String tenantsArray = SPResponse.getString("tenants");
		tenantsArray.trim();
		System.out.println("Friendly name: " + tenantFn);
		System.out.println("Tenants: " + tenantsArray);

		JSONArray tenants = new JSONArray(tenantsArray);
		for (int i = 0; i < tenants.length(); i++) {
			JSONObject tenant = tenants.getJSONObject(i);
			if (tenant.getString("friendlyName").equals(tenantFn)) { //No novo é project
				logger.debug("-- " + tenant.getString("friendlyName"));
				tenantId = tenant.getString("id");
			}
		}
		
		System.out.println("TenantID: " + tenantId);
		
		logger.debug("TenantID: " + tenantId);

		String[] credentials = this.swapTokens(unscopedToken, tenantId);

		logger.debug("token: " + credentials[0]);

		System.out.println("StorageURL: " + credentials[1]);
		setStorageURL(credentials[1]);

		return credentials[0];
	}

	/**
	 * ## Get a scoped token from an unscoped one # @param keystoneEndpoint The
	 * keystone url # @param unscopedToken The unscoped authentication token
	 * obtained from getUnscopedToken() # @param tenanId The tenant Id the user
	 * wants to use def swapTokens(keystoneEndpoint, unscopedToken, tenantId):
	 * data = {'auth' : {'token' : {'id' : unscopedToken}, 'tenantId' :
	 * tenantId}} data = json.dumps(data) req = urllib2.Request(keystoneEndpoint
	 * + "tokens", data, {'Content-Type' : 'application/json'}) resp =
	 * urllib2.urlopen(req) return json.loads(resp.read())
	 * 
	 */
	public String[] swapTokens(String unscopedToken, String tenantId)
			throws UnsupportedEncodingException {

		String[] credentials = { "error", "error" };
		String data = "{\"auth\" : {\"token\" : {\"id\" : \"" + unscopedToken
				+ "\"}, \"tenantId\" : \"" + tenantId + "\"}}";
		
		
		System.out.println("swapTokens: \n"+
				data);

		System.out.println("URL: " + getAuthenticationURL());
		
		HttpPost postRequest = new HttpPost(getAuthenticationURL() + "/tokens");
		StringEntity entity = new StringEntity(data);
		entity.setContentType("application/json");
		postRequest.setHeader("Content-Type", "application/json");
		postRequest.setEntity(entity);
	
		try {
			HttpResponse response = client.execute(postRequest);
			String responseAsString = httpEntityToString(response.getEntity());
			logger.debug("Scoped Token: " + responseAsString);
			System.out.println("Scoped Token: " + responseAsString);
			credentials = getAuthFromJSON(responseAsString);
			return credentials;
		} catch (ClientProtocolException e) {
			logger.error("ClientProtocolException ------ ***", e);
			e.printStackTrace();
			return credentials;
		} catch (IOException e) {
			logger.error("IOException ------ ***", e);
			e.printStackTrace();
			return credentials;
		}
	}

	/**
	 * Converts a HttpEntity to String format
	 * 
	 * @param ent
	 * @return
	 */
	public static String httpEntityToString(HttpEntity ent) {
		try {
			InputStream in = ent.getContent();
			InputStreamReader reader = new InputStreamReader(in);
			BufferedReader bfReader = new BufferedReader(reader);
			String s, content;
			StringBuilder contentBuilder = new StringBuilder();
			while ((s = bfReader.readLine()) != null) {
				contentBuilder.append(s);
			}
			content = contentBuilder.toString();
			// System.out.println("Entity content" + content);
			return content;
		} catch (IOException ex) {
			System.out
					.println("Error while checking keystone authentication response");
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private String getEntityID(String samlRequest)
			throws UnsupportedEncodingException, DataFormatException,
			DecoderException {
		String saml = samlRequest.substring(12, samlRequest.length());
		String samlDecodedURL = URLDecoder.decode(saml, "UTF-8");
		Base64 decoder = new Base64();
		byte[] decodeBytes = decoder.decode(samlDecodedURL);

		Inflater inflater = new Inflater(true);
		inflater.setInput(decodeBytes);
		byte[] xmlMessageBytes = new byte[5000];
		int resultLength = inflater.inflate(xmlMessageBytes);

		if (!inflater.finished()) {
			throw new RuntimeException("didn't allocate enough space to hold "
					+ "decompressed data");
		}

		inflater.end();

		String decodedResponse = new String(xmlMessageBytes, 0, resultLength,
				"UTF-8");

		String entityID = this
				.recuperarEntityID(decodedResponse, "saml:Issuer");

		return entityID;
	}

	private String recuperarEntityID(String fonte, String tagName) {
		String retorno = "";
		if (fonte.contains(tagName)) {
			int ini = fonte.indexOf("<" + tagName);
			int fim = fonte.indexOf("</" + tagName, ini) + tagName.length() + 3;
			String tag = fonte.substring(ini, fim);
			retorno = tag.substring(13, tag.length() - 15);
		}
		return retorno;
	}

	private static String[] getAuthFromJSON(String info) {
		String storageURL = new String();
		String token = new String();
		String[] str = new String[2];

		try {
			JSONObject response = new JSONObject(info);
			JSONObject access = response.getJSONObject("access");

			JSONArray serviceCatalog = access.getJSONArray("serviceCatalog");
			for (int i = 0; i < serviceCatalog.length(); ++i) {
				JSONObject service = serviceCatalog.getJSONObject(i);
				if (service.getString("type").equals("object-store")) {
					JSONArray endpoints = service.getJSONArray("endpoints");
					if (endpoints.length() > 0) {
						storageURL = endpoints.getJSONObject(0).getString(
								"publicURL");
						token = access.getJSONObject("token").getString("id");
						break;
					}
				}
			}
			str[0] = token;
			str[1] = storageURL;

			return str;
		} catch (JSONException ex) {
			System.out.println("Invalid content of authentication");
			return str;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
