package org.embulk.output.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.output.hdfs.transaction.AbortIfExistTx;
import org.embulk.output.hdfs.transaction.DeleteFilesInAdvanceTx;
import org.embulk.output.hdfs.transaction.DeleteRecursiveInAdvanceTx;
import org.embulk.output.hdfs.transaction.OverwriteTx;
import org.embulk.output.hdfs.transaction.ReplaceTx;
import org.embulk.output.hdfs.transaction.Tx;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Locale;

public interface ModeTask
        extends Task
{
    @Config("mode")
    @ConfigDefault("\"abort_if_exist\"")
    Mode getMode();

    void setMode(Mode mode);

    enum Mode
    {
        ABORT_IF_EXIST,
        OVERWRITE,
        DELETE_FILES_IN_ADVANCE,
        DELETE_RECURSIVE_IN_ADVANCE,
        REPLACE;

        private static final Logger logger = Exec.getLogger(Mode.class);

        @Deprecated  // For compat
        public boolean isDefaultMode()
        {
            return this.equals(ABORT_IF_EXIST);
        }

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        @SuppressWarnings("unused")
        public static Mode fromString(String value)
        {
            switch (value) {
                case "abort_if_exist":
                    return ABORT_IF_EXIST;
                case "overwrite":
                    return OVERWRITE;
                case "delete_files_in_advance":
                    return DELETE_FILES_IN_ADVANCE;
                case "delete_recursive_in_advance":
                    return DELETE_RECURSIVE_IN_ADVANCE;
                case "replace":
                    return REPLACE;
                default:
                    throw new ConfigException(String.format(
                            "Unknown mode `%s`. Supported mode is %s",
                            value,
                            Joiner.on(", ").join(
                                    Lists.transform(Lists.newArrayList(Mode.values()), new Function<Mode, String>()
                                    {
                                        @Nullable
                                        @Override
                                        public String apply(@Nullable Mode input)
                                        {
                                            assert input != null;
                                            return String.format("`%s`", input.toString());
                                        }
                                    })
                            )
                    ));
            }
        }

        public Tx newTx()
        {
            switch (this) {
                case ABORT_IF_EXIST:
                    return new AbortIfExistTx();
                case DELETE_FILES_IN_ADVANCE:
                    return new DeleteFilesInAdvanceTx();
                case DELETE_RECURSIVE_IN_ADVANCE:
                    return new DeleteRecursiveInAdvanceTx();
                case OVERWRITE:
                    return new OverwriteTx();
                case REPLACE:
                    return new ReplaceTx();
                default:
                    throw new IllegalStateException();
            }
        }

    }
}
