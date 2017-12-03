package org.embulk.output.hdfs.compat;

import com.google.common.base.Optional;
import org.embulk.config.ConfigException;
import org.embulk.output.hdfs.ModeTask;
import org.embulk.output.hdfs.ModeTask.Mode;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import static org.embulk.output.hdfs.HdfsFileOutputPlugin.PluginTask.DeleteInAdvancePolicy;
import static org.embulk.output.hdfs.ModeTask.Mode.ABORT_IF_EXIST;
import static org.embulk.output.hdfs.ModeTask.Mode.DELETE_FILES_IN_ADVANCE;
import static org.embulk.output.hdfs.ModeTask.Mode.DELETE_RECURSIVE_IN_ADVANCE;
import static org.embulk.output.hdfs.ModeTask.Mode.OVERWRITE;

@Deprecated
public class ModeCompat
{
    private static final Logger logger = Exec.getLogger(ModeCompat.class);

    private ModeCompat()
    {
    }

    @Deprecated
    public static Mode getMode(
            ModeTask task,
            Optional<Boolean> overwrite,
            Optional<DeleteInAdvancePolicy> deleteInAdvancePolicy)
    {
        if (!overwrite.isPresent() && !deleteInAdvancePolicy.isPresent()) {
            return task.getMode();
        }

        // Display Deprecated Messages
        if (overwrite.isPresent()) {
            logger.warn("`overwrite` option is Deprecated. Please use `mode` option instead.");
        }
        if (deleteInAdvancePolicy.isPresent()) {
            logger.warn("`delete_in_advance` is Deprecated. Please use `mode` option instead.");
        }
        if (!task.getMode().isDefaultMode()) {
            String msg = "`mode` option cannot be used with `overwrite` option or `delete_in_advance` option.";
            logger.error(msg);
            throw new ConfigException(msg);
        }


        // Select Mode for Compatibility
        if (!deleteInAdvancePolicy.isPresent()) {
            if (overwrite.get()) {
                logger.warn(String.format("Select `mode: %s` for compatibility", OVERWRITE.toString()));
                return OVERWRITE;
            }
            logger.warn(String.format("Select `mode: %s` for compatibility", ABORT_IF_EXIST.toString()));
            return ABORT_IF_EXIST;
        }

        switch (deleteInAdvancePolicy.get()) {  // deleteInAdvancePolicy is always present.
            case NONE:
                if (overwrite.isPresent() && overwrite.get()) {
                    logger.warn(String.format("Select `mode: %s` for compatibility", OVERWRITE.toString()));
                    return OVERWRITE;
                }
                logger.warn(String.format("Select `mode: %s` for compatibility", ABORT_IF_EXIST.toString()));
                return ABORT_IF_EXIST;
            case FILE_ONLY:
                logger.warn(String.format("Select `mode: %s` for compatibility", DELETE_FILES_IN_ADVANCE.toString()));
                return DELETE_FILES_IN_ADVANCE;
            case RECURSIVE:
                logger.warn(String.format("Select `mode: %s` for compatibility", DELETE_RECURSIVE_IN_ADVANCE.toString()));
                return DELETE_RECURSIVE_IN_ADVANCE;
            default:
                throw new ConfigException(String.format("Unknown policy: %s", deleteInAdvancePolicy.get()));
        }
    }
}
