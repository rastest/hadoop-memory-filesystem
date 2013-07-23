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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LocalInMemoryFileSystemUnitTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private void expectedNullArgumentException(String argName) {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(equalTo(argName + " == null not allowed!"));
	}

	@Test
	public void testLocalizeCacheFilesNullConfiguration() throws IOException {
		expectedNullArgumentException("conf");
		LocalInMemoryFileSystem.localizeCacheFiles(null);
	}

	@Test
	public void testLocalizeCacheFilesFileDoesNotExist()
			throws URISyntaxException, IOException {
		Path file = new Path("/nosuchfile");
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(file.toUri(), conf);

		thrown.expect(FileNotFoundException.class);
		thrown.expectMessage("'" + file + "' not found!");
		LocalInMemoryFileSystem.localizeCacheFiles(conf);
	}

	@Test
	public void testLocalizeCacheFiles() throws IOException {
		String message = "Hello World!";
		Path file = new Path("myfile.txt");
		Configuration conf = new Configuration();
		InMemoryFileSystem.createFile(file, message);
		InMemoryFileSystem.configure(conf);
		DistributedCache.addCacheFile(file.toUri(), conf);
		LocalInMemoryFileSystem.localizeCacheFiles(conf);
		Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);

		assertThat("Wrong number of cache files", localCacheFiles.length,
				is(equalTo(1)));
		Path localFile = localCacheFiles[0];
		FileSystem fs = FileSystem.getLocal(conf);
		assertTrue("Wrong local file sytem: " + fs.getClass().getName(),
				fs instanceof LocalInMemoryFileSystem);
		assertThat("Wrong file contents",
				InMemoryFileSystemUnitTest.readMessage(fs, localFile),
				is(equalTo(message)));
	}
}
