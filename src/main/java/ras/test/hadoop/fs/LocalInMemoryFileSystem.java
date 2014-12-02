/**
 * Copyright 2013 Red Arch Solutions, Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ras.test.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A wrapper for {@link InMemoryFileSystem} which allows it to be the local file
 * system.
 */
public class LocalInMemoryFileSystem extends LocalFileSystem {

	/** The scheme for this file system, "file". */
	public static final String SCHEME = "file";

	/** The URI for this file system. */
	public static final URI NAME = URI.create(SCHEME + ":///");

	/**
	 * The {@link Configuration} key for this file systems implementation class.
	 */
	public static final String CONFIG_IMPL_CLASS_KEY = "fs." + SCHEME + ".impl";

	/**
	 * The base path in which distributed cache files are 'localized' when
	 * {@link #localizeCacheFiles(Configuration) is called.
	 */
	public static final String BASE_LOCAL_CACHE_FILE_DIR = "/mapred/local/taskTracker/distcache/";

	public static final String LOCAL_FILES_KEY = "mapred.cache.localFiles";

	/**
	 * Sets up the <code>conf</code> argument to use this file system as the
	 * local file system so that subsequent calls to methods such as
	 * {@link FileSystem#getLocal(Configuration)} will return an instance of
	 * this file system.
	 * 
	 * @param conf
	 *            The configuration to be modified to use this file system as
	 *            the default.
	 */
	public static void configure(Configuration conf) {
		Validate.notNull(conf, "conf == null not allowed!");

		conf.set(CONFIG_IMPL_CLASS_KEY, LocalInMemoryFileSystem.class.getName());
	}

	/**
	 * 
	 * @param conf
	 *            The configuration whose distributed cache files are to be
	 *            localized.
	 * @throws IOException
	 * @throws IllegalStateException
	 *             If the {@code conf} has not been properly initialized via a
	 *             call to {@link InMemoryFileSystem#configure(Configuration)}.
	 */
	@SuppressWarnings("resource")
	public static void localizeCacheFiles(Configuration conf)
			throws IOException {
		Validate.notNull(conf, "conf == null not allowed!");


		LocalInMemoryFileSystem dstFs = new LocalInMemoryFileSystem();
		dstFs.setConf(conf);
		Path dstPath = new Path(BASE_LOCAL_CACHE_FILE_DIR);
		dstFs.mkdirs(dstPath);

		List<String> localFiles = new ArrayList<String>();
		URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		
		if (cacheFiles == null || cacheFiles.length == 0) {
			throw new IllegalStateException(
					"No files added to distributed cache!");
		}

		for (URI uri : cacheFiles) {
			FileSystem srcFs = FileSystem.get(uri, conf);
			Path srcPath = new Path(uri.getPath());
			localFiles.add(new Path(dstPath, srcPath.getName()).toUri()
					.toString());
			FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, false, conf);
		}
		conf.set(LOCAL_FILES_KEY, StringUtils.join(localFiles, ","));
	}

	public LocalInMemoryFileSystem() {
		super(new InMemoryFileSystem(SCHEME));
		setVerifyChecksum(false);
	}

	@Override
	public void initialize(URI name, Configuration conf) throws IOException {
		super.initialize(name, conf);
		fs.setConf(conf);
		setConf(conf);
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		if (fs != null)
			fs.setConf(conf);
	}
}
