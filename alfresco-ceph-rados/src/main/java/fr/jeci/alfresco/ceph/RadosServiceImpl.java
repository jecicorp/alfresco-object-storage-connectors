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

import java.io.File;
import java.io.IOException;

import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;
import com.ceph.rados.jna.RadosClusterInfo;

import fr.jeci.alfresco.ObjectStorageService;

public class RadosServiceImpl implements ObjectStorageService {
	private static final Log logger = LogFactory.getLog(RadosServiceImpl.class);

	private String configFile;
	private String id;
	private String pool;
	private String metadataPool;
	private String clusterName;

	private Rados rados;

	private int writebufferSize;
	private int readbufferSize;

	public void init() throws RadosException {
		if (logger.isInfoEnabled()) {
			logger.info("Config : " + this.configFile);
			logger.info("Pool : " + this.pool);
			logger.info("clusterName : " + this.clusterName);
			logger.info("id : " + this.id);
		}

		this.rados = new Rados(this.clusterName, this.id, 0);
		File file = new File(this.configFile);
		if (file.exists()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Load Rados Conf " + this.configFile);
			}
			this.rados.confReadFile(file);
		} else {
			logger.error("File " + this.configFile + " not found");
		}
		this.rados.connect();
		logger.info(printRadosStats(rados));
	}

	private String printRadosStats(Rados rados) throws RadosException {
		RadosClusterInfo stat = rados.clusterStat();
		StringBuilder sb = new StringBuilder();
		sb.append("Using RadosServiceImpl");
		sb.append(" (").append(stat.kb).append("KB, ");// Cluster size
		sb.append(stat.kb_used * 100 / stat.kb).append("%, ");
		// sb.append(stat.kb_avail); // KB available
		// sb.append("KB, ");
		sb.append(stat.num_objects).append(")");// Number of objects
		return sb.toString();
	}

	@Override
	public ContentReader getReader(String locator) throws IOException {
		return new CephContentReader(rados, pool, locator);
	}

	@Override
	public ContentWriter getWriter(String locator) {
		return new CephContentWriter(rados, pool, locator);
	}

	@Override
	public boolean isWriteSupported() {
		return true;
	}

	@Override
	public boolean delete(String locator) {
		IoCTX io = null;
		try {
			io = this.rados.ioCtxCreate(pool);
			io.remove(locator);

			return true;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return false;
		} finally {
			if (io != null) {
				this.rados.ioCtxDestroy(io);
			}
		}
	}

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String configFile) {
		this.configFile = configFile;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String getContainer() {
		return pool;
	}

	public void setPool(String pool) {
		this.pool = pool;
	}

	public String getMetadataPool() {
		return metadataPool;
	}

	public void setMetadataPool(String metadataPool) {
		this.metadataPool = metadataPool;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public Rados getRados() {
		return rados;
	}

	public void setRados(Rados rados) {
		this.rados = rados;
	}

	public int getWritebufferSize() {
		return writebufferSize;
	}

	public void setWritebufferSize(int writebufferSize) {
		this.writebufferSize = writebufferSize;
	}

	public int getReadbufferSize() {
		return readbufferSize;
	}

	public void setReadbufferSize(int readbufferSize) {
		this.readbufferSize = readbufferSize;
	}

}
