package org.embulk.output.hdfs;

import java.util.List;
import com.google.common.base.Optional;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;

public class HdfsFileOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        // configuration option 1 (required integer)
        @Config("option1")
        public int getOption1();

        // configuration option 2 (optional string, null is not allowed)
        @Config("optoin2")
        @ConfigDefault("\"myvalue\"")
        public String getOption2();

        // configuration option 3 (optional string, null is allowed)
        @Config("optoin3")
        @ConfigDefault("null")
        public Optional<String> getOption3();

        // usually, run() method needs to write multiple files because size of a file
        // can be very large. So, file name will be:
        //
        //    path_prefix + String.format(sequence_format, taskIndex, sequenceCounterInRunMethod) + file_ext
        //

        //@Config("path_prefix")
        //public String getPathPrefix();

        //@Config("file_ext")
        //public String getFileNameExtension();

        //@Config("sequence_format")
        //@ConfigDefault("\"%03d.%02d.\"")
        //public String getSequenceFormat();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), taskCount, control);

        // non-retryable (non-idempotent) output:
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
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // Write your code here :)
        throw new UnsupportedOperationException("HdfsFileOutputPlugin.open method is not implemented yet");

        // See LocalFileOutputPlugin as an example implementation:
        // https://github.com/embulk/embulk/blob/master/embulk-standards/src/main/java/org/embulk/standards/LocalFileOutputPlugin.java
    }
}
