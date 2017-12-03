package org.embulk.output.hdfs.transaction;

import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.output.hdfs.HdfsFileOutputPlugin.PluginTask;
import org.embulk.output.hdfs.client.HdfsClient;
import org.embulk.output.hdfs.util.SafeWorkspaceName;
import org.embulk.output.hdfs.util.SamplePath;
import org.embulk.output.hdfs.util.StrftimeUtil;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.nio.file.Paths;
import java.util.List;

public class ReplaceTx
        extends AbstractTx
{
    private static final Logger logger = Exec.getLogger(ReplaceTx.class);

    @Override
    protected String getPathPrefix(PluginTask task)
    {
        return Paths.get(task.getSafeWorkspace(), super.getPathPrefix(task)).toString();
    }

    @Override
    protected void beforeRun(PluginTask task)
    {
        HdfsClient hdfsClient = HdfsClient.build(task);
        if (task.getSequenceFormat().contains("/")) {
            throw new ConfigException("Must not include `/` in `sequence_format` if atomic is true.");
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

    @Override
    protected void afterRun(PluginTask task, List<TaskReport> reports)
    {
        HdfsClient hdfsClient = HdfsClient.build(task);
        String outputDir = getOutputSampleDir(task);
        String safeWsWithOutput = Paths.get(task.getSafeWorkspace(), getOutputSampleDir(task)).toString();

        hdfsClient.renameDirectory(safeWsWithOutput, outputDir, true);
        logger.info("Store: {} >>> {}", safeWsWithOutput, outputDir);
    }

    private String getOutputSampleDir(PluginTask task)
    {
        String pathPrefix = StrftimeUtil.strftime(task.getPathPrefix(), task.getRewindSeconds());
        return SamplePath.getDir(pathPrefix, task.getSequenceFormat(), task.getFileExt());
    }

}
