package com.rackspacecloud.client.cloudfiles;

import java.io.IOException;
import java.util.List;

public class FilesClientKeystoneFederatedTestCase {

	static String IDPMAISENDPOINT = "http://idpstcfed.sj.ifsc.edu.br/RNPSecurityTokenService/RNPSTS";
	
	public static void main(String[] args) {

		FilesClientFederatedKeystone federatedAuth = new FilesClientFederatedKeystone(
				new MyHttpClientTrustAll(), "funcionario", "funcionario123",
				"http://cana.ect.ufrn.br:5000/v2.0", null, 1000);

		try {

			// LinkedList<String> idps =
			// federatedAuth.getRealmList("http://cana.ect.ufrn.br:5000/v2.0/tokens");
			List<String> idps = federatedAuth.getRealmList();
			System.out.println("size: " + idps.size());

			for (String idp : idps) {
				System.out.println(idp);
			}

			// String[] response =
			// federatedAuth.getIDPRequest("http://cana.ect.ufrn.br:5000/v2.0/tokens",
			// idps.get(0));
			String[] response = federatedAuth.getIDPRequest(idps.get(0));

			String idpMaisResponse = federatedAuth.getIDPMaisResponse(
					IDPMAISENDPOINT //response[0]
					,response[1]);

			System.out.println("\n idpMais response: " + idpMaisResponse);

			String unscopedTokenJson = federatedAuth.getUnscopedToken(
					idpMaisResponse, idps.get(0));
			System.out.println("\n unscoped token: " + unscopedTokenJson);

			String token = federatedAuth.getScopedToken(unscopedTokenJson,
					"Personal account STCFED");
			System.out.println("Scoped token: " + token);

		} catch (IOException e) {
			System.out.println("error");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("error");
			e.printStackTrace();
		}
	}
}