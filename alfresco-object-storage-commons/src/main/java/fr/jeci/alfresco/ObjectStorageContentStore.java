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
import java.io.Serializable;
import java.util.Collections;

import org.alfresco.repo.content.AbstractContentStore;
import org.alfresco.repo.content.ContentStoreCreatedEvent;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.alfresco.util.GUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Common ContentStore to all commector implementation.
 * 
 * @author jeremie.lesage
 *
 */
public class ObjectStorageContentStore extends AbstractContentStore
		implements ApplicationContextAware, ApplicationListener<ApplicationEvent> {
	private static final Log logger = LogFactory.getLog(ObjectStorageContentStore.class);

	private ObjectStorageService objectStorageService;
	private ApplicationContext applicationContext;

	private String storeProtocole = null;

	@Override
	public boolean isWriteSupported() {
		return this.objectStorageService.isWriteSupported();
	}

	@Override
	public ContentReader getReader(String contentUrl) {
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Content Reader for %s", contentUrl));
		}
		try {
			return this.objectStorageService.getReader(contentUrl);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	@Override
	protected ContentWriter getWriterInternal(final ContentReader existingContentReader, final String newContentUrl) {
		String contentUrl = newContentUrl;
		if (StringUtils.isBlank(contentUrl)) {
			contentUrl = createNewUrl();
		}
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("ContentWriter for %s", contentUrl));
		}
		try {
			return this.objectStorageService.getWriter(contentUrl);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}

	protected String createNewUrl() {
		return String.format("%s://%s/%s", this.storeProtocole, this.objectStorageService.getContainer(),
				GUID.generate());
	}

	@Override
	public boolean delete(String contentUrl) {
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Delete %s", contentUrl));
		}
		return this.objectStorageService.delete(contentUrl);
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		// Once the context has been refreshed, we tell other interested beans
		// about the existence of this content store
		// (e.g. for monitoring purposes)
		if (event instanceof ContextRefreshedEvent && event.getSource() == this.applicationContext) {
			ApplicationContext context = ((ContextRefreshedEvent) event).getApplicationContext();
			context.publishEvent(new ContentStoreCreatedEvent(this, Collections.<String, Serializable>emptyMap()));
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	public void setObjectStorageService(ObjectStorageService objectStorageService) {
		this.objectStorageService = objectStorageService;
	}

	public void setStoreProtocole(String storeProtocole) {
		this.storeProtocole = storeProtocole;
	}

}
