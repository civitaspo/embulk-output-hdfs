package org.embulk.output.hdfs.util;

import java.nio.file.Paths;

public class SamplePath
{
    private SamplePath()
    {
    }

    public static String getFile(String pathPrefix, String sequenceFormat, String fileExt)
    {
        return pathPrefix + String.format(sequenceFormat, 0, 0) + fileExt;
    }

    public static String getDir(String pathPrefix, String sequenceFormat, String fileExt)
    {
        String sampleFile = getFile(pathPrefix, sequenceFormat, fileExt);
        return Paths.get(sampleFile).getParent().toString();
    }
}
