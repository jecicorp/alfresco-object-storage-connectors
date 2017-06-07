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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;

import org.alfresco.service.cmr.repository.ContentIOException;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;

public class RadosReadableByteChannel extends AbstractInterruptibleChannel implements ReadableByteChannel {

	private static final int TRANSFER_SIZE = 8192;
	private byte buf[] = new byte[0];
	private Object readLock = new Object();
	private final String locator;

	private IoCTX io;
	private Rados rados;

	// Offest of bye read from rados
	private int offset = 0;
	// Number of byte read from rados
	private int count;

	public RadosReadableByteChannel(Rados rados, String pool, String locator) {
		this.rados = rados;
		this.locator = locator;

		if (locator == null) {
			throw new IllegalArgumentException("locator is null");
		}

		try {
			this.io = this.rados.ioCtxCreate(pool);
		} catch (RadosException e) {
			throw new ContentIOException("Error creatinf IO Context", e);
		}
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		int len = dst.remaining();
		int totalRead = 0;
		int bytesRead = 0;
		synchronized (readLock) {
			try {
				while (totalRead < len) {
					int bytesToRead = Math.min((len - totalRead), TRANSFER_SIZE);
					if (buf.length < bytesToRead) {
						buf = new byte[bytesToRead];
					}

					try {
						begin();

						this.count = this.io.read(this.locator, bytesToRead, this.offset, this.buf);
						this.offset += this.count;

					} finally {
						end(bytesRead > 0);
					}
					if (bytesRead < 0) {
						break;
					} else {
						totalRead += bytesRead;
					}
					dst.put(buf, 0, bytesRead);
				}
				if ((bytesRead < 0) && (totalRead == 0)) {
					return -1;
				}
			} catch (RadosException e) {
				throw new IOException(e);
			}

			return totalRead;
		}
	}

	@Override
	protected void implCloseChannel() throws IOException {
		this.rados.ioCtxDestroy(io);
	}

}
