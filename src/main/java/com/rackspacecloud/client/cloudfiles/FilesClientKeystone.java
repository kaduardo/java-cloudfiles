/*
 * See COPYING for license information.
 */

package com.rackspacecloud.client.cloudfiles;


import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.json.JSONException;
import org.json.JSONObject;


public class FilesClientKeystone extends FilesClient {

	public FilesClientKeystone() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FilesClientKeystone(HttpClient client, String username,
			String password, String authUrl, String account,
			int connectionTimeOut) {
		super(client, username, password, authUrl, account, connectionTimeOut);
		// TODO Auto-generated constructor stub
	}

	public FilesClientKeystone(String username, String password,
			String authUrl, String account, int connectionTimeOut) {
		super(username, password, authUrl, account, connectionTimeOut);
		// TODO Auto-generated constructor stub
	}

	public FilesClientKeystone(String username, String password, String authUrl) {
		super(username, password, authUrl);
		// TODO Auto-generated constructor stub
	}

	public FilesClientKeystone(String username, String apiAccessKey) {
		super(username, apiAccessKey);
		// TODO Auto-generated constructor stub
	}
	
	public void authenticate()  throws IOException, HttpException {
		
		HttpPost method = new HttpPost(authenticationURL);
        method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
        
        StringEntity entity = new StringEntity(getJSONBody());
        entity.setContentType("application/json");
        method.setEntity(entity);

        FilesResponse2 response = new FilesResponse2(client.execute(method));
        
        if (response.loginSuccess()) {
            isLoggedin = true;
            if(usingSnet() || envSnet()) {
            	storageURL = snetAddr + response.getStorageURL().substring(8);
            } else {
            	storageURL = response.getStorageURL();
            }
            cdnManagementURL = response.getCDNManagementURL();
            authToken = response.getAuthToken();
            logger.debug("storageURL: " + storageURL);
            logger.debug("authToken: " + authToken);
            logger.debug("cdnManagementURL:" + cdnManagementURL);
            logger.debug("ConnectionManager:" + client.getConnectionManager());
        }
        method.abort();

	}
        
    public boolean login() throws IOException, HttpException
    {
    	try
		{
			this.authenticate();
		}
		catch (FilesAuthorizationException e)
		{
			return false;
		}
		return this.isLoggedin();
    }
    
    
    
    public boolean login(String username, String endpoint, String account) throws IOException, HttpException
    {
    	HttpPost method = new HttpPost(endpoint);
        method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
        
        StringEntity entity = new StringEntity(getJSONBody());
        entity.setContentType("application/json");
        method.setEntity(entity);

        FilesResponse2 response = new FilesResponse2(client.execute(method));
        
        if (response.loginSuccess()) {
            isLoggedin = true;
            if(usingSnet() || envSnet()) {
            	storageURL = snetAddr + response.getStorageURL().substring(8);
            } else {
            	storageURL = response.getStorageURL();
            }
            cdnManagementURL = response.getCDNManagementURL();
            authToken = response.getAuthToken();
            logger.debug("storageURL: " + storageURL);
            logger.debug("authToken: " + authToken);
            logger.debug("cdnManagementURL:" + cdnManagementURL);
            logger.debug("ConnectionManager:" + client.getConnectionManager());
        }
        method.abort();

        return this.isLoggedin;
    }
    
    private String getJSONBody() {
        String[] tempArr = username.split(":");
        String userName, tenantName;
        userName = tempArr[0];
        tenantName = tempArr[1];
        
        try {
            JSONObject passwordCredentials = new JSONObject();
            passwordCredentials.put("username", userName);
            passwordCredentials.put("password", password);
            JSONObject auth = new JSONObject();
            auth.put("passwordCredentials", passwordCredentials);
            auth.put("tenantName", tenantName);
            JSONObject obj = new JSONObject();
            obj.put("auth", auth);
            
            return obj.toString();
        } catch (JSONException ex) {
            logger.error("Error when construction authentication body.");
        }

 
        return null;
    }
    
    private boolean envSnet()
	{
		if (System.getenv("RACKSPACE_SERVICENET") == null)
		{
			return false;
		}
		else
		{
			snet = true;
			return true;
		}
	}
	
}
