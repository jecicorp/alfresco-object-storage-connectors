package fr.jeci.alfresco;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public abstract class ByteBufWritableByteChannel extends AbstractInterruptibleChannel implements WritableByteChannel {

	private ByteBuf buffer = Unpooled.buffer(0, Integer.MAX_VALUE);

	private long size = 0L;

	@Override
	public int write(ByteBuffer src) throws IOException {
		int start = this.buffer.writerIndex();
		this.buffer.writeBytes(src);
		int written = this.buffer.writerIndex() - start;
		this.size += written;
		return written;
	}

	@Override
	protected void implCloseChannel() throws IOException {
		try {
			implCloseChannel(this.buffer);
		} finally {
			this.buffer.release();
		}
	}

	/**
	 * @return Total byte written
	 */
	public long getSize() {
		return size;
	}

	protected abstract void implCloseChannel(ByteBuf buffer);

}