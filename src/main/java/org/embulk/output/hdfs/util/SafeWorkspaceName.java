package org.embulk.output.hdfs.util;

import java.nio.file.Paths;
import java.util.UUID;

public class SafeWorkspaceName
{
    private static final String prefix = "embulk-output-hdfs";

    private SafeWorkspaceName()
    {
    }

    public static String build(String workspace)
    {
        long nanoTime = System.nanoTime();
        String uuid = UUID.randomUUID().toString();
        String dirname = String.format("%s_%d_%s", prefix, nanoTime, uuid);
        return Paths.get(workspace, dirname).toString();
    }
}
