package org.embulk.output.hdfs.transaction;

import org.embulk.config.TaskReport;

import java.util.List;

public interface ControlRun
{
    List<TaskReport> run();
}
