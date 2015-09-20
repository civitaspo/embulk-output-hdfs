package org.embulk.output.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

public class HdfsFileOutputPlugin
        implements FileOutputPlugin
{
    private static final Logger logger = Exec.getLogger(HdfsFileOutputPlugin.class);

    public interface PluginTask
            extends Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        public List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        public Map<String, String> getConfig();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("file_ext")
        public String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d.\"")
        public String getSequenceFormat();

        @Config("rewind_seconds")
        @ConfigDefault("0")
        public int getRewindSeconds();

        @Config("overwrite")
        @ConfigDefault("false")
        public boolean getOverwrite();

    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

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
        final String pathSuffix = task.getFileNameExtension();
        final String sequenceFormat = task.getSequenceFormat();

        return new TransactionalFileOutput()
        {
            private final List<String> hdfsFileNames = new ArrayList<>();
            private int fileIndex = 0;
            private OutputStream output = null;

            @Override
            public void nextFile()
            {
                closeCurrentStream();
                Path path = new Path(pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + pathSuffix);
                try {
                    FileSystem fs = getFs(task);
                    output = fs.create(path, task.getOverwrite());
                    logger.info("Uploading '{}'", path);
                }
                catch (IOException e) {
                    logger.error(e.getMessage());
                    throw new RuntimeException(e);
                }
                hdfsFileNames.add(path.toString());
                fileIndex++;
            }

            @Override
            public void add(Buffer buffer)
            {
                try {
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

        for (Object configFile : task.getConfigFiles()) {
            configuration.addResource(configFile.toString());
        }
        configuration.reloadConfiguration();

        for (Map.Entry<String, String> entry: task.getConfig().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
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
}
