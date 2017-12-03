package org.embulk.output.hdfs;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.hdfs.ModeTask.Mode;
import org.embulk.output.hdfs.compat.ModeCompat;
import org.embulk.output.hdfs.transaction.ControlRun;
import org.embulk.output.hdfs.transaction.Tx;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

public class HdfsFileOutputPlugin
        implements FileOutputPlugin
{
    private static final Logger logger = Exec.getLogger(HdfsFileOutputPlugin.class);

    public interface PluginTask
            extends Task, ModeTask
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

        @Deprecated  // Please Use `mode` option
        @Config("overwrite")
        @ConfigDefault("null")
        Optional<Boolean> getOverwrite();

        @Config("doas")
        @ConfigDefault("null")
        Optional<String> getDoas();

        @Deprecated
        enum DeleteInAdvancePolicy{ NONE, FILE_ONLY, RECURSIVE}
        @Deprecated  // Please Use `mode` option
        @Config("delete_in_advance")
        @ConfigDefault("null")
        Optional<DeleteInAdvancePolicy> getDeleteInAdvance();

        @Config("workspace")
        @ConfigDefault("\"/tmp\"")
        String getWorkspace();

        String getSafeWorkspace();
        void setSafeWorkspace(String safeWorkspace);
    }

    private void compat(PluginTask task)
    {
        Mode modeCompat = ModeCompat.getMode(task, task.getOverwrite(), task.getDeleteInAdvance());
        task.setMode(modeCompat);
    }

    // NOTE: This is to avoid the following error.
    // Error: java.lang.RuntimeException: com.fasterxml.jackson.databind.JsonMappingException: Field 'SafeWorkspace' is required but not set
    //  at [Source: N/A; line: -1, column: -1]
    private void avoidDatabindError(PluginTask task)
    {
        task.setSafeWorkspace(task.getWorkspace());
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            final FileOutputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);
        compat(task);
        avoidDatabindError(task);

        Tx tx = task.getMode().newTx();
        return tx.transaction(task, new ControlRun() {
            @Override
            public List<TaskReport> run()
            {
                return control.run(task.dump());
            }
        });
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
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        Tx tx = task.getMode().newTx();
        return tx.newOutput(task, taskSource, taskIndex);
    }
}
