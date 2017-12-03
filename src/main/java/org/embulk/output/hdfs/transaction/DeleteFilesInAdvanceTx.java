package org.embulk.output.hdfs.transaction;

import org.apache.hadoop.fs.Path;
import org.embulk.output.hdfs.HdfsFileOutputPlugin;
import org.embulk.output.hdfs.client.HdfsClient;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class DeleteFilesInAdvanceTx
        extends AbstractTx
{
    private static final Logger logger = Exec.getLogger(DeleteFilesInAdvanceTx.class);

    @Override
    protected void beforeRun(HdfsFileOutputPlugin.PluginTask task)
    {
        HdfsClient hdfsClient = HdfsClient.build(task);
        Path globPath = new Path(getPathPrefix(task) + "*");
        logger.info("Delete {} (File Only) in advance", globPath);
        hdfsClient.globFilesAndTrash(globPath);
    }
}
