package org.embulk.output.hdfs;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.embulk.output.hdfs.HdfsFileOutputPlugin.PluginTask.*;

public class HdfsFileOutputPlugin
        implements FileOutputPlugin
{
    private static final Logger logger = Exec.getLogger(HdfsFileOutputPlugin.class);

    public interface PluginTask
            extends Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        Map<String, String> getConfig();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileExt();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d.\"")
        String getSequenceFormat();

        @Config("rewind_seconds")
        @ConfigDefault("0")
        int getRewindSeconds();

        @Config("overwrite")
        @ConfigDefault("false")
        boolean getOverwrite();

        @Config("doas")
        @ConfigDefault("null")
        Optional<String> getDoas();

        enum DeleteInAdvancePolicy{ NONE, FILE_ONLY, RECURSIVE}
        @Config("delete_in_advance")
        @ConfigDefault("\"NONE\"")
        DeleteInAdvancePolicy getDeleteInAdvance();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        try {
            String pathPrefix = strftime(task.getPathPrefix(), task.getRewindSeconds());
            FileSystem fs = getFs(task);
            deleteInAdvance(fs, pathPrefix, task.getDeleteInAdvance());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            int taskCount,
            FileOutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("hdfs output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final String pathPrefix = strftime(task.getPathPrefix(), task.getRewindSeconds());
        final String pathSuffix = task.getFileExt();
        final String sequenceFormat = task.getSequenceFormat();

        return new TransactionalFileOutput()
        {
            private final List<String> hdfsFileNames = new ArrayList<>();
            private int fileIndex = 0;
            private Path currentPath = null;
            private OutputStream output = null;

            @Override
            public void nextFile()
            {
                closeCurrentStream();
                currentPath = new Path(pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + pathSuffix);
                fileIndex++;
            }

            @Override
            public void add(Buffer buffer)
            {
                try {
                    // this implementation is for creating file when there is data.
                    if (output == null) {
                        FileSystem fs = getFs(task);
                        output = fs.create(currentPath, task.getOverwrite());
                        logger.info("Uploading '{}'", currentPath);
                        hdfsFileNames.add(currentPath.toString());
                    }
                    output.write(buffer.array(), buffer.offset(), buffer.limit());
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    buffer.release();
                }
            }

            @Override
            public void finish()
            {
                closeCurrentStream();
            }

            @Override
            public void close()
            {
                closeCurrentStream();
            }

            @Override
            public void abort()
            {
            }

            @Override
            public TaskReport commit()
            {
                TaskReport report = Exec.newTaskReport();
                report.set("hdfs_file_names", hdfsFileNames);
                return report;
            }

            private void closeCurrentStream()
            {
                if (output != null) {
                    try {
                        output.close();
                        output = null;
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    private static FileSystem getFs(final PluginTask task)
            throws IOException
    {
        Configuration configuration = new Configuration();

        for (String configFile : task.getConfigFiles()) {
            File file = new File(configFile);
            configuration.addResource(file.toURI().toURL());
        }

        for (Map.Entry<String, String> entry: task.getConfig().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }

        if (task.getDoas().isPresent()) {
            URI uri = FileSystem.getDefaultUri(configuration);
            try {
                return FileSystem.get(uri, configuration, task.getDoas().get());
            }
            catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
        return FileSystem.get(configuration);
    }

    private String strftime(final String raw, final int rewind_seconds)
    {
        ScriptingContainer jruby = new ScriptingContainer();
        Object resolved = jruby.runScriptlet(
                String.format("(Time.now - %s).strftime('%s')", String.valueOf(rewind_seconds), raw));
        return resolved.toString();
    }

    private void deleteInAdvance(FileSystem fs, String pathPrefix, DeleteInAdvancePolicy deleteInAdvancePolicy)
            throws IOException
    {
        final Path globPath = new Path(pathPrefix + "*");
        switch (deleteInAdvancePolicy) {
            case NONE:
                // do nothing
                break;
            case FILE_ONLY:
                for (FileStatus status : fs.globStatus(globPath)) {
                    if (status.isFile()) {
                        logger.debug("delete in advance: {}", status.getPath());
                        fs.delete(status.getPath(), false);
                    }
                }
                break;
            case RECURSIVE:
                for (FileStatus status : fs.globStatus(globPath)) {
                    logger.debug("delete in advance: {}", status.getPath());
                    fs.delete(status.getPath(), true);
                }
                break;
            default:
                throw new ConfigException("`delete_in_advance` must not null.");
        }
    }
}
