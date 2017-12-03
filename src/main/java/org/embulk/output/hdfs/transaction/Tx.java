package org.embulk.output.hdfs.transaction;

import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskSource;
import org.embulk.output.hdfs.HdfsFileOutputPlugin.PluginTask;
import org.embulk.spi.TransactionalFileOutput;

public interface Tx
{
    ConfigDiff transaction(PluginTask task, ControlRun control);

    TransactionalFileOutput newOutput(PluginTask task, TaskSource taskSource, int taskIndex);
}
