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

import java.io.IOException;

import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import fr.jeci.alfresco.ObjectStorageService;
import io.openio.sds.Client;
import io.openio.sds.ClientBuilder;
import io.openio.sds.Settings;
import io.openio.sds.models.OioUrl;

public class OpenIOServiceImpl implements ObjectStorageService {
	private static final Log logger = LogFactory.getLog(OpenIOServiceImpl.class);

	private String target;
	private String namespace;
	private String account;
	private Client client;
	private String container;

	public void init() {
		Settings settings = new Settings();
		settings.proxy().ns(namespace).url(target);
		client = ClientBuilder.newClient(settings);
	}

	@Override
	public boolean isWriteSupported() {
		// this.container.getcontainerWritePermission()
		return true;
	}

	@Override
	public ContentReader getReader(String contentUrl) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("GETTING OBJECT - BUCKET: " + container + " KEY: " + contentUrl);
		}
		OioUrl urlFile = OioUrl.url(account, container, contentUrl);
		return new OpenIOContentReader(this.client, urlFile);
	}

	@Override
	public ContentWriter getWriter(String contentUrl) throws IOException {
		OioUrl urlFile = OioUrl.url(account, container, contentUrl);
		return new OpenIOContentWriter(client, urlFile);
	}

	@Override
	public boolean delete(String contentUrl) {
		try {
			OioUrl urlFile = OioUrl.url(account, container, contentUrl);
			client.deleteObject(urlFile);
			if (logger.isDebugEnabled()) {
				logger.debug("Deleting object from OpenIO with url: " + contentUrl);
			}
			return true;
		} catch (Exception e) {
			logger.error("Error deleting OpenIO Object", e);
		}
		return false;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public Client getClient() {
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	@Override
	public String getContainer() {
		return container;
	}

	public void setContainer(String container) {
		this.container = container;
	}

}
