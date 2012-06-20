/**
 *
 */
package com.rackspacecloud.client.cloudfiles;

import org.apache.http.Header;
import org.apache.http.StatusLine;

/**
 * @author lvaughn
 */
public class FilesNotFoundException extends FilesException
{
	private static final long serialVersionUID = 111718445621236026L;

	/**
     * @param message		The message
   	 * @param httpHeaders	The returned HTTP headers
   	 * @param httpStatusLine The HTTP Status lined returned
	 */
	public FilesNotFoundException(String message, Header[] httpHeaders,
								  StatusLine httpStatusLine)
	{
		super(message, httpHeaders, httpStatusLine);
	}

}
