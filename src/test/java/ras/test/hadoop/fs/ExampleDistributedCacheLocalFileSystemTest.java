package ras.test.hadoop.fs;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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
 * An example of using the InMemoryFileSystem to to test distributed cache
 */
@SuppressWarnings("deprecation")
public class ExampleDistributedCacheLocalFileSystemTest {

	public Path messageFile = new Path("message.txt");
	
	public class MapperExample implements Mapper<Text, Text, Text, Text> {
		public String message = null;

		public void configure(JobConf conf) {
			
			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(conf);
				assertNotNull("Local Cache Files are null", paths);
				assertThat("Wrong number of files in distributed cache!", paths.length, is(equalTo(1)));
				
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

	@Before
	public void setUp(){
		InMemoryFileSystem.resetFileSystemState();
	}
	
	@After
	public void tearDown() throws IOException {
		InMemoryFileSystem.resetFileSystemState();
	}

	@Test
	public void test() throws IOException {
		JobConf conf = new JobConf();
		String message = "Hello World!";
		InMemoryFileSystem.configure(conf);
		InMemoryFileSystem.createFile(messageFile, message);
		
		DistributedCache.addCacheFile(messageFile.toUri(), conf);
		LocalInMemoryFileSystem.localizeCacheFiles(conf);
		
		MapperExample mapper = new MapperExample();
		mapper.configure(conf);
		assertThat("Wrong message", mapper.message, is(equalTo(message)));
	}}
