package org.embulk.output.hdfs.transaction;

public class OverwriteTx
        extends AbstractTx
{
    @Override
    protected boolean canOverwrite()
    {
        return true;
    }
}
