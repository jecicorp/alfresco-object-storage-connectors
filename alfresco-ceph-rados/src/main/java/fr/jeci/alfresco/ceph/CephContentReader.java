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
import java.nio.channels.ReadableByteChannel;

import org.alfresco.repo.content.AbstractContentReader;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;
import com.ceph.rados.jna.RadosObjectInfo;

public class CephContentReader extends AbstractContentReader {
	private static final Log logger = LogFactory.getLog(CephContentReader.class);

	private final Rados rados;
	private final String pool;
	private final String locator;

	public CephContentReader(Rados rados, String pool, String locator) throws IOException {
		super(locator);
		this.rados = rados;
		this.locator = locator;
		this.pool = pool;

		if (locator == null) {
			throw new IllegalArgumentException("locator is null");
		}

		if (pool == null) {
			throw new IllegalArgumentException("pool is null");
		}
		}

	}

	@Override
	public boolean exists() {
		IoCTX ioctx = null;
		try {
			ioctx = this.rados.ioCtxCreate(pool);
			RadosObjectInfo stat = ioctx.stat(locator);
			return stat != null;

		} catch (RadosException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (ioctx != null) {
				this.rados.ioCtxDestroy(ioctx);
			}
		}

		return false;
	}

	@Override
	public long getLastModified() {
		IoCTX ioctx = null;

		try {
			ioctx = this.rados.ioCtxCreate(this.pool);
			RadosObjectInfo stat = ioctx.stat(this.locator);
			
			if (stat != null) {
				return stat.getMtime();
			}

		} catch (RadosException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (ioctx != null) {
				this.rados.ioCtxDestroy(ioctx);
			}
		}
		
		return 0L;
	}

	@Override
	public long getSize() {
		IoCTX ioctx = null;

		try {
			ioctx = this.rados.ioCtxCreate(this.pool);
			RadosObjectInfo stat = ioctx.stat(this.locator);

			if (stat != null) {
				return stat.getSize();
			}

		} catch (RadosException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (ioctx != null) {
				this.rados.ioCtxDestroy(ioctx);
			}
		}

		return 0L;
	}

	@Override
	protected ContentReader createReader() {
		try {
			return new CephContentReader(this.rados, this.pool, this.locator);
		} catch (IOException e) {
			throw new ContentIOException(e.getMessage(), e);
		}
	}

	@Override
	protected ReadableByteChannel getDirectReadableChannel() {
		return new RadosReadableByteChannel(this.rados, this.pool, this.locator);
	}

}
