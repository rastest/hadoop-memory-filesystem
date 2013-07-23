package ras.test.hadoop.fs;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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

@SuppressWarnings("deprecation")
public class ExampleDefaultFileSystemTest {

	public Path messageFile = new Path("message.txt");

	public class MapperExample implements Mapper<Text, Text, Text, Text> {
		public String message = null;

		public void configure(JobConf conf) {
			try {
				
				// Get the default file system...
				FileSystem fs = FileSystem.get(conf);
				
				//Check its class...
				if (!(fs instanceof InMemoryFileSystem)) {
					fail("Wrong file system: " + fs.getClass().getName());
				}
				
				// Read from the default file system...
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(fs.open(messageFile)));
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
		
		// Initialize the configuration so that the InMemory file system is the default...
		InMemoryFileSystem.configure(conf);
		
		// Generate a simple text file in the file system...
		InMemoryFileSystem.createFile(messageFile, message);
		
		
		MapperExample mapper = new MapperExample();
		mapper.configure(conf);
		assertThat("Wrong message", mapper.message, is(equalTo(message)));
	}
}
