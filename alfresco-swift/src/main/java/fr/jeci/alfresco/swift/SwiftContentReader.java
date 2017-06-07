package fr.jeci.alfresco.swift;

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
import org.javaswift.joss.model.StoredObject;

public class SwiftContentReader extends AbstractContentReader implements ContentReader {

	private StoredObject storedObject;

	public SwiftContentReader(StoredObject object) {
		super(object.getName());
		this.storedObject = object;
	}

	@Override
	protected ContentReader createReader() {
		return new SwiftContentReader(this.storedObject);
	}

	@Override
	protected ReadableByteChannel getDirectReadableChannel() {
		if (!exists()) {
			throw new ContentIOException("Content object does not exist on Swift");
		}

		return Channels.newChannel(this.storedObject.downloadObjectAsInputStream());
	}

	@Override
	public boolean exists() {
		return this.storedObject.exists();
	}

	@Override
	public long getLastModified() {
		if (!exists()) {
			return 0L;
		}

		return this.storedObject.getLastModifiedAsDate().getTime();
	}

	@Override
	public long getSize() {
		if (!exists()) {
			return 0L;
		}

		return this.storedObject.getContentLength();
	}

}
