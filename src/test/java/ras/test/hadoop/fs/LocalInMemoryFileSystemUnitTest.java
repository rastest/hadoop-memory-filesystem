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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LocalInMemoryFileSystemUnitTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private final Configuration conf = new Configuration();
	
	@Before
	public void setUp(){
		// Initialize the configuration...
		InMemoryFileSystem.configure(conf);
	}
	
	@After
	public void tearDown(){
		// Free up any state associated with configuration file system context.
		InMemoryFileSystem.resetFileSystemState(conf);
	}
	
	@Test
	public void testLocalizeCacheFilesNullConfiguration() throws IOException {
		expectedNullArgumentException("conf");
		LocalInMemoryFileSystem.localizeCacheFiles(null);
	}
 
	@Test
	public void testLocalizedCacheFilesUninitializedConfiguration()
			throws IOException {
		expectedIllegalStateException("The filesystem has not been properly configured! The configuration has no in-memory file system context.");
		LocalInMemoryFileSystem.localizeCacheFiles(new Configuration());
	}
	
	@Test
	public void testLocalizedCacheFilesNoFilesAddedToCache() throws IOException{
		expectedIllegalStateException("No files added to distributed cache!");
		LocalInMemoryFileSystem.localizeCacheFiles(conf);
	}

	@Test
	public void testLocalizeCacheFilesFileDoesNotExist()
			throws URISyntaxException, IOException {
		Path file = new Path("/nosuchfile");
		Configuration conf = new Configuration();
		InMemoryFileSystem.configure(conf);
		DistributedCache.addCacheFile(file.toUri(), conf);

		thrown.expect(FileNotFoundException.class);
		thrown.expectMessage("'" + file + "' not found!");
		LocalInMemoryFileSystem.localizeCacheFiles(conf);
	}

	@Test
	public void testLocalizeCacheFiles() throws IOException {
		String message = "Hello World!";
		Path file = new Path("myfile.txt");
		FileSystem defaultFs = InMemoryFileSystem.get(conf);
		InMemoryFileSystem.createFile(defaultFs, file, message);
		DistributedCache.addCacheFile(file.toUri(), conf);
		LocalInMemoryFileSystem.localizeCacheFiles(conf);
		Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);

		assertThat("Wrong number of cache files", localCacheFiles.length,
				is(equalTo(1)));
		Path localFile = localCacheFiles[0];
		FileSystem localFs = FileSystem.getLocal(conf);
		assertTrue("Wrong local file sytem: " + localFs.getClass().getName(),
				localFs instanceof LocalInMemoryFileSystem);
		assertThat("Wrong file contents",
				InMemoryFileSystemUnitTest.readMessage(localFs, localFile),
				is(equalTo(message)));
	}

	//
	// End of tests
	//

	private void expectedNullArgumentException(String argName) {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(equalTo(argName + " == null not allowed!"));
	}

	private void expectedIllegalStateException(String message) {
		thrown.expect(IllegalStateException.class);
		thrown.expectMessage(equalTo(message));
	}
}
