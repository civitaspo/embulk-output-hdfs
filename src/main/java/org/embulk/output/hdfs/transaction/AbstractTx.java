package org.embulk.output.hdfs.transaction;

import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.hdfs.HdfsFileOutput;
import org.embulk.output.hdfs.util.StrftimeUtil;
import org.embulk.spi.Exec;
import org.embulk.spi.TransactionalFileOutput;

import java.util.List;

import static org.embulk.output.hdfs.HdfsFileOutputPlugin.PluginTask;

abstract class AbstractTx
        implements Tx
{
    protected void beforeRun(PluginTask task)
    {
    }

    protected void afterRun(PluginTask task, List<TaskReport> reports)
    {
    }

    protected ConfigDiff newConfigDiff()
    {
        return Exec.newConfigDiff();
    }

    public ConfigDiff transaction(PluginTask task, ControlRun control)
    {
        beforeRun(task);
        List<TaskReport> reports = control.run();
        afterRun(task, reports);
        return newConfigDiff();
    }

    protected String getPathPrefix(PluginTask task)
    {
        return StrftimeUtil.strftime(task.getPathPrefix(), task.getRewindSeconds());
    }

    protected boolean canOverwrite()
    {
        return false;
    }

    public TransactionalFileOutput newOutput(PluginTask task, TaskSource taskSource, int taskIndex)
    {
        return new HdfsFileOutput(task, getPathPrefix(task), canOverwrite(), taskIndex);
    }
}
