package org.embulk.output;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class HdfsOutputPlugin implements FileOutputPlugin
{
    private static final Logger logger = Exec.getLogger(HdfsOutputPlugin.class);

    public interface PluginTask extends Task
    {
        @Config("config_files")
        @ConfigDefault("[]")
        public List<String> getConfigFiles();

        @Config("config")
        @ConfigDefault("{}")
        public Map<String, String> getConfig();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d\"")
        public String getSequenceFormat();

        @Config("output_path")
        @ConfigDefault("\"/tmp/embulk.working.hdfs_output.%Y%m%d_%s\"")
        public String getOutputPath();

        @Config("working_path")
        @ConfigDefault("\"/tmp/embulk.working.hdfs_output.%Y%m%d_%s\"")
        public String getWorkingPath();

    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  int taskCount,
                                  FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileOutputPlugin.Control control)
    {
        control.run(taskSource);
        return Exec.newConfigDiff();
    }


    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<CommitReport> successCommitReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        Configuration configuration = getHdfsConfiguration(task);
        FileSystem fs = getFs(configuration);
        String workingPath = strftime(task.getWorkingPath());
        return new TransactionalHdfsFileOutput(task, fs, workingPath, taskIndex);
    }

    private Configuration getHdfsConfiguration(final PluginTask task)
    {
        Configuration configuration = new Configuration();

        List configFiles = task.getConfigFiles();
        for (Object configFile : configFiles) {
            configuration.addResource(configFile.toString());
        }

        for (Map.Entry<String, String> entry: task.getConfig().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }

        return configuration;
    }

    private FileSystem getFs(final Configuration configuration) {
        try {
            FileSystem fs = FileSystem.get(configuration);
            return fs;
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    private String strftime(final String path)
    {
        // strftime
        ScriptingContainer jruby = new ScriptingContainer();
        Object result = jruby.runScriptlet("Time.now.strftime('" + path + "')");
        return result.toString();
    }

    static class TransactionalHdfsFileOutput implements TransactionalFileOutput
    {
        private final int taskIndex;
        private final FileSystem fs;
        private final String workingPath;
        private final String sequenceFormat;

        private int fileIndex = 0;
        private int callCount = 0;
        private Path currentPath = null;
        private OutputStream currentStream = null;

        public TransactionalHdfsFileOutput(PluginTask task, FileSystem fs, String workingPath, int taskIndex)
        {
            this.taskIndex = taskIndex;
            this.fs = fs;
            this.workingPath = workingPath;
            this.sequenceFormat = task.getSequenceFormat();
        }

        public void nextFile() {
            closeCurrentStream();
            currentPath = new Path(workingPath + String.format(sequenceFormat, taskIndex, fileIndex));
            try {
                if (fs.exists(currentPath)) {
                    throw new IllegalAccessException(currentPath.toString() + "already exists.");
                }
                currentStream = fs.create(
                    currentPath,
                    new Progressable() {
                        @Override
                        public void progress() {
                            logger.info("{} byte written.", 1);
                        }
                    });
                logger.info("Uploading '{}'", currentPath.toString());
            }
            catch (IOException | IllegalAccessException e) {
                logger.error(e.getMessage());
                throw Throwables.propagate(e);
            }
            fileIndex++;
        }

        @Override
        public void add(Buffer buffer) {
            if (currentStream == null) {
                throw new IllegalStateException("nextFile() must be called before poll()");
            }
            try {
                logger.debug("#add called {} times for taskIndex {}", callCount, taskIndex);
                currentStream.write(buffer.array(), buffer.offset(), buffer.limit());
                callCount++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                buffer.release();
            }
        }

        @Override
        public void finish() {
            closeCurrentStream();
        }

        @Override
        public void close() {
            closeCurrentStream();
        }

        @Override
        public void abort() {
        }

        @Override
        public CommitReport commit() {
            CommitReport report = Exec.newCommitReport();
            report.set("files", currentPath);
            return report;
        }

        private void closeCurrentStream() {
            try {
                if (currentStream != null) {
                    currentStream.close();
                    currentStream = null;
                }

                callCount = 0;
            } catch (IOException e) {
                logger.error(e.getMessage());
                throw Throwables.propagate(e);
            }
        }
    }
}
