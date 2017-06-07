package fr.jeci.alfresco.openio;

/*  Copyright 2016, 2017 - Jeci SARL - http://jeci.fr

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see http://www.gnu.org/licenses/. 
 */

import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.alfresco.repo.content.AbstractContentReader;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.openio.sds.Client;
import io.openio.sds.models.ObjectInfo;
import io.openio.sds.models.OioUrl;

/**
 * Created by manens on 23/07/16.
 */
public class OpenIOContentReader extends AbstractContentReader {
	private static final Log logger = LogFactory.getLog(OpenIOContentReader.class);

	private Client client;
	private OioUrl oioUrl;

	protected OpenIOContentReader(Client client, OioUrl urlFile) {
		super(urlFile.object());
		this.client = client;
		this.oioUrl = urlFile;
	}

	@Override
	protected ContentReader createReader() {
		if (logger.isDebugEnabled()) {
			logger.debug("Called createReader for contentUrl -> " + getContentUrl());
		}
		return new OpenIOContentReader(this.client, this.oioUrl);
	}

	@Override
	protected ReadableByteChannel getDirectReadableChannel() {
		if (!exists()) {
			throw new ContentIOException("Content object does not exist on OpenIO");
		}

		try {
			final ObjectInfo fileObjectMetadata = client.getObjectInfo(this.oioUrl);
			return Channels.newChannel(this.client.downloadObject(fileObjectMetadata));
		} catch (Exception e) {
			throw new ContentIOException("Unable to retrieve content object from OpenIO", e);
		}

	}

	@Override
	public boolean exists() {
		return client.getObjectInfo(this.oioUrl) != null;
	}

	@Override
	public long getLastModified() {
		if (!exists()) {
			return 0L;
		}

		final ObjectInfo fileObjectMetadata = client.getObjectInfo(this.oioUrl);
		return fileObjectMetadata.ctime();
	}

	@Override
	public long getSize() {
		if (!exists()) {
			return 0L;
		}
		
		final ObjectInfo fileObjectMetadata = client.getObjectInfo(this.oioUrl);
		return fileObjectMetadata.size();
	}

}
