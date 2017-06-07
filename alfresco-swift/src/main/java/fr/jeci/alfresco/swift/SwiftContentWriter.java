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

import java.nio.channels.WritableByteChannel;

import org.alfresco.repo.content.AbstractContentWriter;
import org.alfresco.service.cmr.repository.ContentReader;
import org.javaswift.joss.model.StoredObject;

public class SwiftContentWriter extends AbstractContentWriter {
	private StoredObject storedObject;
	private long size = 0L;

	public SwiftContentWriter(StoredObject object) {
		super(object.getName(), null);
		this.storedObject = object;
	}

	@Override
	protected ContentReader createReader() {
		return new SwiftContentReader(this.storedObject);
	}

	@Override
	protected WritableByteChannel getDirectWritableChannel() {
		return new SwiftChannel(storedObject);
	}

	@Override
	public long getSize() {
		return this.size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public StoredObject getStoredObject() {
		return storedObject;
	}
}
