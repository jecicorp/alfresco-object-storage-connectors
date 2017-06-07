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

import java.io.IOException;

import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.apache.commons.lang3.StringUtils;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import fr.jeci.alfresco.ObjectStorageService;

public class SwiftServiceImpl implements ObjectStorageService {

	private String username;
	private String password;
	private String url;
	private String tenantId;
	private String tenantName;
	private String containerName;
	private Container container;

	public void init() {

		AccountConfig config = new AccountConfig();
		config.setAuthenticationMethod(AuthenticationMethod.BASIC);
		config.setUsername(username);
		config.setPassword(password);
		config.setAuthUrl(url);
		if (StringUtils.isNotEmpty(tenantId)) {
			config.setTenantId(tenantId);
		}
		if (StringUtils.isNotEmpty(tenantName)) {
			config.setTenantName(tenantName);
		}
		Account account = new AccountFactory(config).createAccount();
		this.container = account.getContainer(containerName);
	}

	@Override
	public boolean isWriteSupported() {
		// this.container.getcontainerWritePermission()
		return true;
	}

	@Override
	public ContentReader getReader(String contentUrl) throws IOException {
		StoredObject object = this.container.getObject(contentUrl);
		return new SwiftContentReader(object);
	}

	@Override
	public ContentWriter getWriter(String contentUrl) throws IOException {
		StoredObject object = this.container.getObject(contentUrl);
		return new SwiftContentWriter(object);
	}

	@Override
	public boolean delete(String contentUrl) {
		StoredObject object = this.container.getObject(contentUrl);
		object.delete();
		return true;
	}

	@Override
	public String getContainer() {
		return container.getName();
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public String getTenantName() {
		return tenantName;
	}

	public void setTenantName(String tenantName) {
		this.tenantName = tenantName;
	}

	public void setContainer(String containerName) {
		this.containerName = containerName;
	}
}
