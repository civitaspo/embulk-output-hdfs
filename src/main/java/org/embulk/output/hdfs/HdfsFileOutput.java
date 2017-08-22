package org.embulk.output.hdfs;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.embulk.config.TaskReport;
import org.embulk.output.hdfs.client.HdfsClient;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;

public class HdfsFileOutput
        implements FileOutput, TransactionalFileOutput
{
    private static final Logger logger = Exec.getLogger(HdfsFileOutput.class);
    private final RetryExecutor re = RetryExecutor.retryExecutor()
            .withRetryLimit(3)
            .withMaxRetryWait(500) // ms
            .withMaxRetryWait(10 * 60 * 1000); // ms

    private final HdfsClient hdfsClient;
    private final int taskIdx;
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String fileExt;
    private final boolean overwrite;

    private int fileIdx = 0;
    private Path currentPath = null;
    private OutputStream o = null;

    HdfsFileOutput(HdfsFileOutputPlugin.PluginTask task, String pathPrefix, int taskIdx)
    {
        this.hdfsClient = HdfsClient.build(task);
        this.pathPrefix = pathPrefix;
        this.taskIdx = taskIdx;
        this.sequenceFormat = task.getSequenceFormat();
        this.fileExt = task.getFileExt();
        this.overwrite = task.getOverwrite();
    }

    HdfsFileOutput(HdfsFileOutputPlugin.PluginTask task, String workspace, String pathPrefix, int taskIdx)
    {
        this(task, Paths.get(workspace, pathPrefix).toString(), taskIdx);
    }

    @Override
    public void abort()
    {
    }

    @Override
    public TaskReport commit()
    {
        return Exec.newTaskReport();
    }

    @Override
    public void nextFile()
    {
        closeCurrentStream();
        currentPath = newPath();
        fileIdx++;
    }

    @Override
    public void add(Buffer buffer)
    {
        try {
            // this implementation is for creating file when there is data.
            if (o == null) {
                o = hdfsClient.create(currentPath, overwrite);
                logger.info("Uploading '{}'", currentPath);
            }
            write(buffer);
        }
        catch (RetryExecutor.RetryGiveupException e) {
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
        hdfsClient.close();
    }

    private void write(final Buffer buffer)
            throws RetryExecutor.RetryGiveupException
    {
        re.run(new RetryExecutor.Retryable<Void>() {
            @Override
            public Void call()
                    throws Exception
            {
                o.write(buffer.array(), buffer.offset(), buffer.limit());
                return null;
            }

            @Override
            public boolean isRetryableException(Exception exception)
            {
                return true; // TODO: which Exception is retryable?
            }

            @Override
            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                    throws RetryExecutor.RetryGiveupException
            {
                String m = String.format(
                        "%s. (Retry: Count: %d, Limit: %d, Wait: %d ms)",
                        exception.getMessage(),
                        retryCount,
                        retryLimit,
                        retryWait);
                logger.warn(m, exception);
            }

            @Override
            public void onGiveup(Exception firstException, Exception lastException)
                    throws RetryExecutor.RetryGiveupException
            {
            }
        });
    }

    private Path newPath()
    {
        return new Path(pathPrefix + getSequence() + fileExt);
    }

    private String getSequence()
    {
        return String.format(sequenceFormat, taskIdx, fileIdx);
    }

    private void closeCurrentStream()
    {
        if (o != null) {
            try {
                o.close();
                o = null;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
