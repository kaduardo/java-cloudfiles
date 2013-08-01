/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package br.rnp.stcfed.sts.client.impl;

/**
 *
 * @author marlonguerios
 */
class RNPSecurityTokenException extends Exception {

    public RNPSecurityTokenException(Throwable cause) {
        super(cause);
    }

    public RNPSecurityTokenException(String message, Throwable cause) {
        super(message, cause);
    }

    public RNPSecurityTokenException(String message) {
        super(message);
    }

    public RNPSecurityTokenException() {
    }

}
