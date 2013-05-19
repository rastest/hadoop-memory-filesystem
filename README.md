InMemoryFileSystem
========================

This is a memory based implementation of the `hadoop.fs.FileSystem`. It is designed to be used for testing code written for the Hadoop environment. The following example shows how to test a Mapper using the `InMemoryFileSystem`. 

    package ras.test.hadoop.fs;

    import static org.hamcrest.CoreMatchers.equalTo;
    import static org.hamcrest.CoreMatchers.is;
    import static org.junit.Assert.assertThat;
    import static org.junit.Assert.fail;

    import java.io.BufferedReader;
    import java.io.BufferedWriter;
    import java.io.IOException;
    import java.io.InputStreamReader;
    import java.io.OutputStreamWriter;

    import org.apache.hadoop.fs.FileSystem;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapred.JobConf;
    import org.apache.hadoop.mapred.Mapper;
    import org.apache.hadoop.mapred.OutputCollector;
    import org.apache.hadoop.mapred.Reporter;
    import org.junit.After;
    import org.junit.Test;

    import ras.test.hadoop.fs.InMemoryFileSystem;

    @SuppressWarnings("deprecation")
    public class InMemoryFileSystemExampleUseCase1Test {

        public Path messageFile = new Path("message.txt");

        public class MapperExample implements Mapper<Text, Text, Text, Text> {
            public String message = null;

            public void configure(JobConf conf) {
                try {
                    FileSystem fs = FileSystem.get(conf);
                    if (!(fs instanceof InMemoryFileSystem)) {
                        fail("Wrong file system: " + fs.getClass().getName());
                    }
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(messageFile)));
                    this.message = reader.readLine();
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            public void close() throws IOException {
            }

            public void map(Text arg0, Text arg1, OutputCollector<Text, Text> arg2,	Reporter arg3) throws IOException {
            }
        }

        @After
        public void tearDown() throws IOException {
            InMemoryFileSystem.resetFileSystemState();
        }

        @Test
        public void test() throws IOException {
            JobConf conf = new JobConf();
            InMemoryFileSystem mFs = InMemoryFileSystem.get(conf);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(mFs.create(messageFile)));
            String message = "Hello World!";
            writer.write(message);
            writer.flush();
            writer.close();
            MapperExample mapper = new MapperExample();
            mapper.configure(conf);
            assertThat("Wrong message", mapper.message, is(equalTo(message)));
        }
    }
