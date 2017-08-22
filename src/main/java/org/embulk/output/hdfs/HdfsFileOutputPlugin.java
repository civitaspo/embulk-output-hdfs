package org.embulk.output.hdfs;

import com.google.common.base.Optional;
import org.apache.hadoop.fs.Path;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.hdfs.client.HdfsClient;
import org.embulk.output.hdfs.util.SafeWorkspaceName;
import org.embulk.output.hdfs.util.SamplePath;
import org.embulk.output.hdfs.util.StrftimeUtil;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.embulk.output.hdfs.HdfsFileOutputPlugin.PluginTask.DeleteInAdvancePolicy;

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

        @Config("atomic")
        @ConfigDefault("false")
        boolean getAtomic();

        @Config("workspace")
        @ConfigDefault("\"/tmp\"")
        String getWorkspace();

        String getSafeWorkspace();
        void setSafeWorkspace(String safeWorkspace);

        @ConfigInject
        ScriptingContainer getJruby();
    }

    private void configure(PluginTask task)
    {
        HdfsClient hdfsClient = HdfsClient.build(task);

        if (task.getAtomic()) {
            if (task.getSequenceFormat().contains("/")) {
                throw new ConfigException("Must not include `/` in `sequence_format` if atomic is true.");
            }
            if (!task.getDeleteInAdvance().equals(DeleteInAdvancePolicy.NONE)) {
                throw new ConfigException("`delete_in_advance` must be `NONE` if atomic is true.");
            }
            if (task.getOverwrite()) {
                logger.info("Overwrite directory {} if exists.", getOutputSampleDir(task));
            }

            String safeWorkspace = SafeWorkspaceName.build(task.getWorkspace());
            logger.info("Use as a workspace: {}", safeWorkspace);

            String safeWsWithOutput = Paths.get(safeWorkspace, getOutputSampleDir(task)).toString();
            logger.debug("The actual workspace must be with output dirs: {}", safeWsWithOutput);
            if (!hdfsClient.mkdirs(safeWsWithOutput)) {
                throw new ConfigException(String.format("Failed to make a directory: %s", safeWsWithOutput));
            }
            task.setSafeWorkspace(safeWorkspace);
        }

        String pathPrefix = StrftimeUtil.strftime(task.getPathPrefix(), task.getRewindSeconds());
        deleteInAdvance(hdfsClient, pathPrefix, task.getDeleteInAdvance());
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        configure(task);

        control.run(task.dump());

        if (task.getAtomic()) {
            atomicRename(task);
        }

        return Exec.newConfigDiff();
    }

    private void atomicRename(PluginTask task)
    {
        HdfsClient hdfsClient = HdfsClient.build(task);
        String outputDir = getOutputSampleDir(task);
        String safeWsWithOutput = Paths.get(task.getSafeWorkspace(), getOutputSampleDir(task)).toString();

        hdfsClient.renameDirectory(safeWsWithOutput, outputDir, task.getOverwrite());
        logger.info("Store: {} >>> {}", safeWsWithOutput, outputDir);
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
        String pathPrefix = StrftimeUtil.strftime(task.getPathPrefix(), task.getRewindSeconds());;
        if (task.getAtomic()) {
            return new HdfsFileOutput(task, task.getSafeWorkspace(), pathPrefix, taskIndex);
        }
        else {
            return new HdfsFileOutput(task, pathPrefix, taskIndex);
        }
    }

    private String getOutputSampleDir(PluginTask task)
    {
        String pathPrefix = StrftimeUtil.strftime(task.getPathPrefix(), task.getRewindSeconds());
        return SamplePath.getDir(pathPrefix, task.getSequenceFormat(), task.getFileExt());
    }

    private void deleteInAdvance(
            HdfsClient hdfsClient,
            String pathPrefix,
            DeleteInAdvancePolicy deleteInAdvancePolicy)
    {
        final Path globPath = new Path(pathPrefix + "*");
        switch (deleteInAdvancePolicy) {
            case NONE:
                // do nothing
                break;
            case FILE_ONLY:
                logger.info("Delete {} (File Only) in advance", globPath);
                hdfsClient.globAndRemove(globPath);
                break;
            case RECURSIVE:
                logger.info("Delete {} (Recursive) in advance", globPath);
                hdfsClient.globAndRemoveRecursive(globPath);
                break;
            default:
                throw new ConfigException("`delete_in_advance` must not null.");
        }
    }
}
