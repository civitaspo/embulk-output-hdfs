package org.embulk.output.hdfs;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.OutputPlugin.Control;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.io.Files.readLines;
import static org.embulk.output.hdfs.HdfsFileOutputPlugin.*;
import static org.embulk.spi.type.Types.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newString;

public class TestHdfsFileOutputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private Logger logger = runtime.getExec().getLogger(TestHdfsFileOutputPlugin.class);
    private HdfsFileOutputPlugin plugin;
    private FileOutputRunner runner;
    private String pathPrefix;

    private static final Schema SCHEMA = new Schema.Builder()
            .add("_c0", BOOLEAN)
            .add("_c1", LONG)
            .add("_c2", DOUBLE)
            .add("_c3", STRING)
            .add("_c4", TIMESTAMP)
            .add("_c5", JSON)
            .build();

    @Before
    public void createResources()
            throws IOException
    {
        plugin = new HdfsFileOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(HdfsFileOutputPlugin.class));
        pathPrefix = tmpFolder.getRoot().getAbsolutePath() + "/embulk-output-hdfs_";
    }

    private ConfigSource getBaseConfigSource()
    {
        return Exec.newConfigSource()
                .set("type", "hdfs")
                .set("path_prefix", pathPrefix)
                .set("file_ext", "csv")
                .setNested("formatter", Exec.newConfigSource()
                        .set("type", "csv")
                        .set("newline", "CRLF")
                        .set("newline_in_field", "LF")
                        .set("header_line", true)
                        .set("charset", "UTF-8")
                        .set("quote_policy", "NONE")
                        .set("quote", "\"")
                        .set("escape", "\\")
                        .set("null_string", "")
                        .set("default_timezone", "UTC"));
    }

    @Test
    public void testDefaultValues()
    {
        ConfigSource config = getBaseConfigSource();
        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(pathPrefix, task.getPathPrefix());
        assertEquals("csv", task.getFileExt());
        assertEquals("%03d.%02d.", task.getSequenceFormat());
        assertEquals(Lists.newArrayList(), task.getConfigFiles());
        assertEquals(Maps.newHashMap(), task.getConfig());
        assertEquals(0, task.getRewindSeconds());
        assertEquals(false, task.getOverwrite());
        assertEquals(Optional.absent(), task.getDoas());
        assertEquals(PluginTask.DeleteInAdvancePolicy.NONE, task.getDeleteInAdvance());
    }

    @Test(expected = ConfigException.class)
    public void testRequiredValues()
    {
        ConfigSource config = Exec.newConfigSource();
        PluginTask task = config.loadConfig(PluginTask.class);
    }

    private List<String> lsR(List<String> names, java.nio.file.Path dir)
    {
        try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(dir)) {
            for (java.nio.file.Path path : stream) {
                if (path.toFile().isDirectory()) {
                    logger.debug("[lsR] find a directory: {}", path.toAbsolutePath().toString());
                    names.add(path.toAbsolutePath().toString());
                    lsR(names, path);
                }
                else {
                    logger.debug("[lsR] find a file: {}", path.toAbsolutePath().toString());
                    names.add(path.toAbsolutePath().toString());
                }
            }
        }
        catch (IOException e) {
            logger.debug(e.getMessage(), e);
        }
        return names;
    }

    private void run(ConfigSource config)
    {
        runner.transaction(config, SCHEMA, 1, new Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                TransactionalPageOutput pageOutput = runner.open(taskSource, SCHEMA, 1);
                boolean committed = false;
                try {
                    // Result:
                    // _c0,_c1,_c2,_c3,_c4,_c5
                    // true,2,3.0,45,1970-01-01 00:00:00.678000 +0000,{\"k\":\"v\"}
                    // true,2,3.0,45,1970-01-01 00:00:00.678000 +0000,{\"k\":\"v\"}
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), SCHEMA,
                            true, 2L, 3.0D, "45", Timestamp.ofEpochMilli(678L), newMap(newString("k"), newString("v")),
                            true, 2L, 3.0D, "45", Timestamp.ofEpochMilli(678L), newMap(newString("k"), newString("v")))) {
                        pageOutput.add(page);
                    }
                    pageOutput.commit();
                    committed = true;
                }
                finally {
                    if (!committed) {
                        pageOutput.abort();
                    }
                    pageOutput.close();
                }
                return Lists.newArrayList();
            }
        });
    }

    private void assertRecordsInFile(String filePath)
    {
        try {
            List<String> lines = readLines(new File(filePath),
                    Charsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                String[] record = lines.get(i).split(",");
                if (i == 0) {
                    for (int j = 0; j <= 4; j++) {
                        assertEquals("_c" + j, record[j]);
                    }
                }
                else {
                    // true,2,3.0,45,1970-01-01 00:00:00.678000 +0000
                    assertEquals("true", record[0]);
                    assertEquals("2", record[1]);
                    assertEquals("3.0", record[2]);
                    assertEquals("45", record[3]);
                    assertEquals("1970-01-01 00:00:00.678000 +0000", record[4]);
                    assertEquals("{\"k\":\"v\"}", record[5]);
                }
            }
        }
        catch (IOException e) {
            logger.debug(e.getMessage(), e);
        }
    }

    @Test
    public void testBulkLoad()
    {
        ConfigSource config = getBaseConfigSource()
                .setNested("config", Exec.newConfigSource()
                        .set("fs.hdfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                        .set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                        .set("fs.defaultFS", "file:///"));

        run(config);
        List<String> fileList = lsR(Lists.<String>newArrayList(), Paths.get(tmpFolder.getRoot().getAbsolutePath()));
        assertThat(fileList, hasItem(containsString(pathPrefix + "001.00.csv")));
        assertRecordsInFile(String.format("%s/%s001.00.csv",
                tmpFolder.getRoot().getAbsolutePath(),
                pathPrefix));
    }

    @Test
    public void testDeleteRECURSIVEInAdvance()
            throws IOException
    {
        for (int n = 0; n <= 10; n++) {
            tmpFolder.newFile("embulk-output-hdfs_file_" + n + ".txt");
            tmpFolder.newFolder("embulk-output-hdfs_directory_" + n);
        }

        List<String> fileListBeforeRun = lsR(Lists.<String>newArrayList(), Paths.get(tmpFolder.getRoot().getAbsolutePath()));

        ConfigSource config = getBaseConfigSource()
                .setNested("config", Exec.newConfigSource()
                        .set("fs.hdfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                        .set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                        .set("fs.defaultFS", "file:///"))
                .set("delete_in_advance", "RECURSIVE");

        run(config);

        List<String> fileListAfterRun = lsR(Lists.<String>newArrayList(), Paths.get(tmpFolder.getRoot().getAbsolutePath()));
        assertNotEquals(fileListBeforeRun, fileListAfterRun);
        assertThat(fileListAfterRun, not(hasItem(containsString("embulk-output-hdfs_directory"))));
        assertThat(fileListAfterRun, not(hasItem(containsString("txt"))));
        assertThat(fileListAfterRun, hasItem(containsString(pathPrefix + "001.00.csv")));
        assertRecordsInFile(String.format("%s/%s001.00.csv",
                tmpFolder.getRoot().getAbsolutePath(),
                pathPrefix));
    }

    @Test
    public void testDeleteFILE_ONLYInAdvance()
            throws IOException
    {
        for (int n = 0; n <= 10; n++) {
            tmpFolder.newFile("embulk-output-hdfs_file_" + n + ".txt");
            tmpFolder.newFolder("embulk-output-hdfs_directory_" + n);
        }

        List<String> fileListBeforeRun = lsR(Lists.<String>newArrayList(), Paths.get(tmpFolder.getRoot().getAbsolutePath()));

        ConfigSource config = getBaseConfigSource()
                .setNested("config", Exec.newConfigSource()
                        .set("fs.hdfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                        .set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                        .set("fs.defaultFS", "file:///"))
                .set("delete_in_advance", "FILE_ONLY");

        run(config);

        List<String> fileListAfterRun = lsR(Lists.<String>newArrayList(), Paths.get(tmpFolder.getRoot().getAbsolutePath()));
        assertNotEquals(fileListBeforeRun, fileListAfterRun);
        assertThat(fileListAfterRun, not(hasItem(containsString("txt"))));
        assertThat(fileListAfterRun, hasItem(containsString("embulk-output-hdfs_directory")));
        assertThat(fileListAfterRun, hasItem(containsString(pathPrefix + "001.00.csv")));
        assertRecordsInFile(String.format("%s/%s001.00.csv",
                tmpFolder.getRoot().getAbsolutePath(),
                pathPrefix));
    }
}
