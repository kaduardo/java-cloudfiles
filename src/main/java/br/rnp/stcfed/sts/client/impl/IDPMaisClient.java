/**
 * Esta classe faz parte do prot��tipo do Servi��o de Tradu����o de Credencials Federadas do
 * GT-STCFed - Grupo de Trabalho da RNP.
 *
 * Favor n��o distribuir sem autoriza����o da RNP ou do grupo de trabalho supra-citado.
 */
package br.rnp.stcfed.sts.client.impl;

import br.rnp.stcfed.sts.STSConstants;
import br.rnp.stcfed.sts.client.STSClient;
import br.rnp.stcfed.sts.tools.RNPClaims;
import com.sun.xml.ws.api.security.trust.WSTrustException;

import org.apache.commons.codec.binary.Base64;
import org.opensaml.Configuration;
import org.opensaml.saml1.core.Assertion;
import org.opensaml.xml.io.Unmarshaller;
import org.opensaml.xml.io.UnmarshallerFactory;
import org.opensaml.xml.io.UnmarshallingException;
import org.opensaml.xml.util.XMLHelper;
import org.w3c.dom.Element;

/**
 * Esta classe demonstra como se conectar ao STS da RNP e obter uma asser����o
 * para utiliza����o com outros servi��os
 *
 * Duas formas de conex��o ao STS s��o demonstradas abaixo:
 *
 *  - Conex��o direta
 *      - Neste caso o cliente se conecta diretamente ao STS e solicita um token
 *      que poder�� ser utilizado posteriormente da forma que o cliente preferir.
 *
 *  - Conex��o impl��cita atrav��s das pol��ticas de seguran��a de um servi��o
 *      - Neste caso, o servi��o que o cliente est�� tentando acessar possui uma
 *      politica de seguran��a que informa o STS no qual ele confia.
 *
 *      Dessa forma, utilizando o framework Metro, no momento em que o cliente acessa o
 *      servi��o sem as credenciais necess��rias, uma requisi����o ao STS �� feita
 *      automaticamente para a obten����o de um token que �� inserido no contexto
 *      da mensagem SOAP a ser enviada ao servi��o.
 *
 *      Abaixo est�� a pol��tica de seguran��a do servi��o acessado neste exemplo:
 *
 *           <sp:SupportingTokens>
 *                    <wsp:Policy>
 *                       <!-- sp:SamlToken sp:IncludeToken="http://schemas.xmlsoap.org/ws/2005/07/securitypolicy/IncludeToken/AlwaysToRecipient">
 *                           <wsp:Policy>
 *                               <sp:WssSamlV11Token10/>
 *                           </wsp:Policy>
 *                       </sp:SamlToken -->
 *                       <sp:IssuedToken sp:IncludeToken="http://docs.oasis-open.org/ws-sx/ws-securitypolicy/200702/IncludeToken/AlwaysToRecipient">
 *                           <sp:RequestSecurityTokenTemplate>
 *                               <t:TokenType>http://docs.oasis-open.org/wss/oasis-wss-saml-token-profile-1.1#SAMLV1.1</t:TokenType>
 *                           </sp:RequestSecurityTokenTemplate>
 *                           <sp:Issuer>
 *                               <wsaw:Address>http://gtstcfed.sj.ifsc.edu.br:8081/RNPSecurityTokenService/RNPSTS</wsaw:Address>
 *                               <wsaw:Metadata>
 *                                   <wsx:Metadata>
 *                                       <wsx:MetadataSection>
 *                                           <wsx:MetadataReference>
 *                                               <wsaw:Address>http://gtstcfed.sj.ifsc.edu.br:8081/RNPSecurityTokenService/RNPSTS/mex</wsaw:Address>
 *                                           </wsx:MetadataReference>
 *                                       </wsx:MetadataSection>
 *                                   </wsx:Metadata>
 *                               </wsaw:Metadata>
 *                           </sp:Issuer>
 *                           <t:Claims Dialect="http://schemas.xmlsoap.org/ws/2005/05/identity"
 *                                     xmlns:ic="http://schemas.xmlsoap.org/ws/2005/05/identity">
 *                               <ic:ClaimType Uri="http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname"/>
 *                               <ic:ClaimType Uri="http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname" />
 *                               <ic:ClaimType Uri="http://schemas.xmlsoap.org/ws/2005/05/identity/claims/locality"/>
 *                               <ic:ClaimType Uri="http://schemas.xmlsoap.org/ws/2005/05/identity/claims/role"/>
 *                           </t:Claims>
 *                       </sp:IssuedToken>
 *                   </wsp:Policy>
 *               </sp:SupportingTokens>
 *
 * @author marlonguerios
 */
public class IDPMaisClient {

    // Indica o endpoint do STS
    private String stsEndpoint = "http://idpstcfed.sj.ifsc.edu.br/RNPSecurityTokenService/RNPSTS";
    // Indica o servi��o que o cliente deseja acessar
    private String serviceAppliesTo = "http://cana.ect.ufrn.br:5000/v2.0/tokens";
    // Indica o nome do usu��rio no servidor de identidade desejado
    private String username = "funcionario";
    // Indica a senha do usu��rio no servidor de identidade desejado
    private String password = "funcionario123";
    
    
//    
//    private String username = "joaogt";
//    // Indica a senha do usu��rio no servidor de identidade desejado
//    private String password = "joao123";

    
//     
//    private String username = "aluno";
//    // Indica a senha do usu��rio no servidor de identidade desejado
//    private String password = "aluno123";
    // Indica o servidor de identidade que deve ser utilizado para autentica����o
    // e obten����o de atributos do usu��rio
    private String idpId = "ifsc";
    
    private String tokenType;
    
    public static void main(String[] args) throws Exception {
        // prepare client
        IDPMaisClient rnpStsClient = new IDPMaisClient();
        // Nome de usu��rio da federa����o
//        rnpStsClient.setUsername("aluno");
//        // Senha do usu��rio da federa����o
//        rnpStsClient.setPassword("aluno123");
        // Provedor de identidade IdP+ da institui����o de origem
        rnpStsClient.setStsEndpoint("http://idpstcfed.sj.ifsc.edu.br/RNPSecurityTokenService/RNPSTS");
        
        // Autentica o usu��rio e obt��m asser����o SAML com dados do usu��rio
        Element securityTokenFromSts = rnpStsClient.getTokenFromSts();
        System.out.println(XMLHelper.prettyPrintXML(securityTokenFromSts));
        Base64 decoder = new  Base64();
        
        System.out.println("\nSAMLResponse base64\n\n"+new String(decoder.encode(XMLHelper.prettyPrintXML(securityTokenFromSts).getBytes())));
    }
    
    public String getIDPMaisSAMLResponse(String usuario, String senha, String StsEndpoint, String serviceAppliesTo) throws WSTrustException{
    	
    	setStsEndpoint(StsEndpoint);
    	setUsername(usuario);
    	setPassword(senha);
    	setServiceAppliesTo(serviceAppliesTo);
    	
    	
    	 // Autentica o usu��rio e obt��m asser����o SAML com dados do usu��rio
        Element securityTokenFromSts = getTokenFromSts();
        System.out.println(XMLHelper.prettyPrintXML(securityTokenFromSts));
        Base64 decoder = new  Base64();
        
       String samlResponse = new String(decoder.encode(XMLHelper.prettyPrintXML(securityTokenFromSts).getBytes()));
    	
    	
    	return samlResponse;
    }
    
    /**
     * Este m��todo acessa um STS para obten����o de um Token.
     * @return Retorna um objeto do tipo org.w3c.dom.Element com o token
     * @throws WSTrustException
     */
    public Element getTokenFromSts() throws WSTrustException {
        // Cria uma inst��ncia de STSClient, um cliente padr��o para acesso ao
        // STS da RNP
        STSClient stsClient = new DefaultSTSClient();
        // Indica o endere��o do STS
        stsClient.setStsEndpoint(stsEndpoint);
        // Indica o servidor de identidade desejado para valida����o do usu��rio
        stsClient.setActAsIdpId(idpId);
        // Indica o usu��rio do servidor de indentidade informado
        stsClient.setActAsUsername(username);
        // Indica a senha do usu��rio no servidor de identidade informado
        stsClient.setActAsPassword(password);
        // Indica o servi��o para o qual este token ser�� utilizado. O STS pode ser
        // configurado de forma diferente para cada servi��o ao qual ele atender��,
        // por isso �� necess��rio informar o servi��o para o qual ser�� utilizado
        // o token gerado.
        stsClient.setServiceAppliesTo(serviceAppliesTo);
        // Indica o tipo de token sendo requisitado.
        // O STS da RNP �� capaz de gerar SAML1, SAML2 e X509
        //stsClient.setTokenType(STSConstants.SAML11_ASSERTION_TOKEN_TYPE);
        //stsClient.setTokenType(STSConstants.SAML11_ASSERTION_TOKEN_TYPE);
        stsClient.setTokenType(STSConstants.SAML20_ASSERTION_TOKEN_TYPE);
        stsClient.getSTSConfig().setKeyType("http://docs.oasis-open.org/ws-sx/ws-trust/200512/Bearer");

        // Indica quais atributos do usu��rio devem ser retornados no token
        RNPClaims claims = new RNPClaims();
        claims.addClaimType(RNPClaims.IC_GIVENNAME);
        claims.addClaimType(RNPClaims.IC_SURNAME);
        claims.addClaimType(RNPClaims.BR_EDU_AFFILIATION_TYPE);
//
        stsClient.setClaims(claims);

        System.out.println("getTokenFromSts finalizado");

        // Efetua a requisi����o ao STS e obt��m o token
        return stsClient.getIssuedToken();
    }

    /**
     * O m��todo abaixo �� um exemplo de como um Element pode ser convertido em um
     * objeto Assertion do OpenSAML
     *
     * Importante: Observar o tipo de token, SAML1 ou SAML2
     * @param assertionElement Elemento contendo o token gerado pelo STS
     * @return Retorna um objeto Assertion
     * @throws UnmarshallingException
     */
    public Assertion getSaml1Assertion(Element assertionElement) throws UnmarshallingException {
        UnmarshallerFactory unmarshallerFactory = Configuration.getUnmarshallerFactory();
        Unmarshaller unmarshaller = unmarshallerFactory.getUnmarshaller(assertionElement);
        Assertion saml1Assertion = (Assertion) unmarshaller.unmarshall(assertionElement);
        return saml1Assertion;
    }

    public String getIdpId() {
        return idpId;
    }

    public void setIdpId(String idpId) {
        this.idpId = idpId;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getStsEndpoint() {
        return stsEndpoint;
    }

    public void setStsEndpoint(String stsEndpoint) {
        this.stsEndpoint = stsEndpoint;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getServiceAppliesTo() {
        return serviceAppliesTo;
    }

    public void setServiceAppliesTo(String serviceAppliesTo) {
        this.serviceAppliesTo = serviceAppliesTo;
    }

    public String getTokenType() {
        return tokenType;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }
 
}
