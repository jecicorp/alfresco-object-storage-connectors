package fr.jeci.alfresco.ceph;

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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import org.alfresco.repo.content.AbstractContentWriter;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;

import com.ceph.rados.Rados;

public class CephContentWriter extends AbstractContentWriter {
	private final Rados rados;
	private final String pool;
	private final String locator;

	private final RadosWritableByteChannel channel;

	public CephContentWriter(Rados rados, String pool, String locator) {
		super(locator, null);
		this.rados = rados;
		this.pool = pool;
		this.locator = locator;

		if (locator == null) {
			throw new IllegalArgumentException("locator is null");
		}

		if (pool == null) {
			throw new IllegalArgumentException("pool is null");
		}
		this.channel = new RadosWritableByteChannel(rados, pool, locator);
	}

	@Override
	protected ContentReader createReader() {
		try {
			return new CephContentReader(rados, pool, locator);
		} catch (IOException e) {
			throw new ContentIOException(e.getMessage(), e);
		}
	}

	@Override
	protected WritableByteChannel getDirectWritableChannel() {
		return this.channel;
	}

	@Override
	public long getSize() {
		return this.channel.getOffset();
	}

}
