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

import org.javaswift.joss.model.StoredObject;

import fr.jeci.alfresco.ByteBufWritableByteChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

public class SwiftChannel extends ByteBufWritableByteChannel {

	private StoredObject storedObject;

	public SwiftChannel(StoredObject storedObject) {
		this.storedObject = storedObject;
	}

	@Override
	protected void implCloseChannel(ByteBuf buffer) {
		storedObject.uploadObject(new ByteBufInputStream(buffer));
	}

}
