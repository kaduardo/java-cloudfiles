package com.rackspacecloud.client.cloudfiles;

import org.apache.http.Header;
import org.apache.http.StatusLine;

/**
 * @version $Id:$
 */
public class FilesContainerNotFoundException extends FilesException
{
    private static final long serialVersionUID = 7751467778430037798L;

    /**
     * @param message        The message
     * @param httpHeaders    The returned HTTP headers
     * @param httpStatusLine The HTTP Status lined returned
     */
    public FilesContainerNotFoundException(String message, Header[] httpHeaders,
                                           StatusLine httpStatusLine) {
        super(message, httpHeaders, httpStatusLine);
    }
}
