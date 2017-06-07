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

import java.nio.channels.WritableByteChannel;

import org.alfresco.repo.content.AbstractContentWriter;
import org.alfresco.service.cmr.repository.ContentReader;

import io.openio.sds.Client;
import io.openio.sds.models.OioUrl;

/**
 * Created by manens on 23/07/16.
 */
public class OpenIOContentWriter extends AbstractContentWriter {
	private Client client;
	private OioUrl oioUrl;

	private final OpenIOChannel channel;

	protected OpenIOContentWriter(Client client, OioUrl urlFile) {
		super(urlFile.object(), null);
		this.client = client;
		this.oioUrl = urlFile;

		this.channel = new OpenIOChannel(this.client, this.oioUrl);
	}

	@Override
	protected ContentReader createReader() {
		return new OpenIOContentReader(this.client, this.oioUrl);
	}

	@Override
	protected WritableByteChannel getDirectWritableChannel() {
		return this.channel;
	}

	@Override
	public long getSize() {
		return this.channel.getSize();
	}

}
