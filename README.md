InMemoryFileSystem
========================

This is a memory based implementation of the `hadoop.fs.FileSystem`. It is 
designed to be used for testing code written for the Hadoop environment. The 
following example shows how to test a Mapper using the `InMemoryFileSystem`. 

package ras.test.hadoop.fs;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * An example of using the InMemoryFileSystem to test distributed cache
 */
public class ExampleDistributedCacheLocalFileSystemTest {

	private final Path messageFile = new Path("message.txt");

	private final Configuration conf = new Configuration();

	@Before
	public void setUp() {
		// Initialize the configuration...
		InMemoryFileSystem.configure(conf);
	}

	@After
	public void tearDown() throws IOException {
		// Release the static file system state associated with the
		// configuration...
		InMemoryFileSystem.resetFileSystemState(conf);
	}

	@Test
	public void test() throws IOException {
		final String message = "Hello World!";

		// Get the default file system...
		FileSystem defaultFs = FileSystem.get(conf);

		// Create a file in the default file system...
		InMemoryFileSystem.createFile(defaultFs, messageFile, message);

		// Make sure the file exists in the default file system...
		assertTrue("File does not exist in the default file system!",
				defaultFs.exists(messageFile));

		// Add the file to the distributed cache...
		DistributedCache.addCacheFile(messageFile.toUri(), conf);

		// Manually force the file to be moved to the local file system & update
		// the configuration with the location of the file in the local file
		// system...
		LocalInMemoryFileSystem.localizeCacheFiles(conf);

		// Test to see if a mapper can find and load the file from the local
		// file system based on the configuration...
		MapperExample mapper = new MapperExample();
		mapper.configure(new JobConf(conf));

		// Test that the mapper successfully loaded the file...
		assertThat("Wrong message", mapper.message, is(equalTo(message)));
	}

	//
	// End tests
	//

	/**
	 * A Mapper for testing
	 */
	public class MapperExample implements Mapper<Text, Text, Text, Text> {
		public String message = null;

		public void configure(JobConf conf) {

			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(conf);
				assertNotNull("Local Cache Files are null", paths);
				assertThat("Wrong number of files in distributed cache!",
						paths.length, is(equalTo(1)));

				FileSystem fs = FileSystem.getLocal(conf);
				if (!(fs instanceof LocalInMemoryFileSystem)) {
					fail("Wrong file system: " + fs.getClass().getName());
				}
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(fs.open(paths[0])));
				this.message = reader.readLine();
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public void close() throws IOException {
		}

		public void map(Text arg0, Text arg1, OutputCollector<Text, Text> arg2,
				Reporter arg3) throws IOException {
		}
	}
}
