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
import java.nio.channels.WritableByteChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;

import org.alfresco.service.cmr.repository.ContentIOException;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;

public class RadosWritableByteChannel extends AbstractInterruptibleChannel implements WritableByteChannel {

	private IoCTX io = null;

	private byte[] buf = new byte[0];
	// 128k
	private static final int TRANSFER_SIZE = 131072;
	private Object writeLock = new Object();
	private final String locator;
	private final Rados rados;
	private int offset = 0;

	public RadosWritableByteChannel(Rados rados, String pool, String locator) {
		this.locator = locator;
		this.rados = rados;
		try {
			this.io = this.rados.ioCtxCreate(pool);
		} catch (RadosException e) {
			throw new ContentIOException("Error creatinf IO Context", e);
		}
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		int len = src.remaining();
		int totalWritten = 0;
		synchronized (this.writeLock) {
			try {
				while (totalWritten < len) {
					int bytesToWrite = Math.min((len - totalWritten), TRANSFER_SIZE);
					if (this.buf.length != bytesToWrite) {
						this.buf = new byte[bytesToWrite];
					}

					src.get(this.buf, 0, bytesToWrite);
					try {
						begin();
						this.io.write(this.locator, this.buf, this.offset);
						this.offset += bytesToWrite;
					} finally {
						end(bytesToWrite > 0);
					}
					totalWritten += bytesToWrite;
				}
				
				if (this.offset == 0) {
					/* Create empty object */
					this.io.write(this.locator, "", this.offset);
				}
			} catch (RadosException e) {
				throw new IOException(e);
			}
			return totalWritten;
		}
	}

	@Override
	protected void implCloseChannel() throws IOException {
		if (this.io != null) {
			this.rados.ioCtxDestroy(this.io);
		}
	}

}
