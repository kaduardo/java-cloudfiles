/*
 * See COPYING for license information.
 */

package com.rackspacecloud.client.cloudfiles;

import java.util.HashMap;
import java.util.Map;

public class FilesObjectMetaData extends FilesMetaData {

    /**
	 * An object storing the metadata for an FS Cloud object
	 *
	 * @param mimeType	  The MIME type for the object
	 * @param contentLength The content-length (e.g., size) of the object
	 * @param eTag		  The MD5 check-sum of the object's contents
	 * @param lastModified  The last time the object was modified.
	 */
	public FilesObjectMetaData(String mimeType, String contentLength, String eTag, String lastModified)
	{
        super(mimeType, lastModified, contentLength, eTag);
    }
}
