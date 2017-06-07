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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

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

	// NUM_LOCKS absolutely must be a power of 2 for the use of locks to be
	// evenly balanced
	private static final int NUM_LOCKS = 256;
	private static final ReentrantReadWriteLock[] LOCKS;

	private ObjectStorageService objectStorageService;
	private ApplicationContext applicationContext;

	private String storeProtocole = null;

	static {
		LOCKS = new ReentrantReadWriteLock[NUM_LOCKS];
		for (int i = 0; i < NUM_LOCKS; i++) {
			LOCKS[i] = new ReentrantReadWriteLock();
		}
	}

	@Override
	public boolean isWriteSupported() {
		return this.objectStorageService.isWriteSupported();
	}

	@Override
	public ContentReader getReader(String contentUrl) {
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Content Reader for %s", contentUrl));
		}

		// Use pool of locks - which one is determined by a hash of the URL.
		// This will stop the content from being read/cached multiple times from
		// the backing store
		// when it should only be read once - cached versions should be returned
		// after that.
		ReadLock readLock = readWriteLock(contentUrl).readLock();
		readLock.lock();
		try {
			return this.objectStorageService.getReader(contentUrl);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} finally {
			readLock.unlock();
		}

		return null;
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
		}

		return null;
	}

	@Override
	public boolean isContentUrlSupported(String contentUrl) {
		String startUrl = String.format("%s://%s/", this.storeProtocole, this.objectStorageService.getContainer());
		return contentUrl.startsWith(startUrl);
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
		ReentrantReadWriteLock readWriteLock = readWriteLock(contentUrl);
		ReadLock readLock = readWriteLock.readLock();
		readLock.lock();
		try {
			return this.objectStorageService.delete(contentUrl);
		} finally {
			readLock.unlock();
		}
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

	/**
	 * Get a ReentrantReadWriteLock for a given URL. The lock is from a pool
	 * rather than per URL, so some contention is expected.
	 * 
	 * @param url
	 *            String
	 * @return ReentrantReadWriteLock
	 */
	public ReentrantReadWriteLock readWriteLock(String url) {
		return LOCKS[lockIndex(url)];
	}

	private int lockIndex(String url) {
		return url.hashCode() & (NUM_LOCKS - 1);
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
