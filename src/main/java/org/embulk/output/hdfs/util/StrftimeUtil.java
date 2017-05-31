package org.embulk.output.hdfs.util;

import org.jruby.embed.ScriptingContainer;

public class StrftimeUtil
{
    private static final String scriptTemplate = "(Time.now - %d).strftime('%s')";

    private StrftimeUtil()
    {
    }

    public static String strftime(String format, int rewindSeconds)
    {
        String script = buildScript(format, rewindSeconds);
        return new ScriptingContainer().runScriptlet(script).toString();
    }

    private static String buildScript(String format, int rewindSeconds)
    {
        return String.format(scriptTemplate, rewindSeconds, format);
    }
}
