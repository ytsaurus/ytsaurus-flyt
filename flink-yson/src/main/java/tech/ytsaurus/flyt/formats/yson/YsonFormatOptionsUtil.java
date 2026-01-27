package tech.ytsaurus.flyt.formats.yson;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import static tech.ytsaurus.flyt.formats.yson.YsonFormatOptions.FAIL_ON_MISSING_FIELD;
import static tech.ytsaurus.flyt.formats.yson.YsonFormatOptions.IGNORE_PARSE_ERRORS;
import static tech.ytsaurus.flyt.formats.yson.YsonFormatOptions.TIMESTAMP_FORMAT;

/** Base on JsonFormatOptions */
@Internal
public class YsonFormatOptionsUtil {

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final Set<String> TIMESTAMP_FORMAT_ENUM =
            new HashSet<>(Arrays.asList(SQL, ISO_8601));

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static TimestampFormat getTimestampFormat(ReadableConfig config) {
        String timestampFormat = config.get(TIMESTAMP_FORMAT);
        switch (timestampFormat) {
            case SQL:
                return TimestampFormat.SQL;
            case ISO_8601:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for yson decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        boolean failOnMissingField = tableOptions.get(FAIL_ON_MISSING_FIELD);
        boolean ignoreParseErrors = tableOptions.get(IGNORE_PARSE_ERRORS);
        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(
                    FAIL_ON_MISSING_FIELD.key()
                            + " and "
                            + IGNORE_PARSE_ERRORS.key()
                            + " shouldn't both be true.");
        }
        validateTimestampFormat(tableOptions);
    }

    /** Validator for yson encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        validateTimestampFormat(tableOptions);
    }

    /** Validates timestamp format which value should be SQL or ISO-8601. */
    static void validateTimestampFormat(ReadableConfig tableOptions) {
        String timestampFormat = tableOptions.get(TIMESTAMP_FORMAT);
        if (!TIMESTAMP_FORMAT_ENUM.contains(timestampFormat)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for %s. Supported values are [SQL, ISO-8601].",
                            timestampFormat, TIMESTAMP_FORMAT.key()));
        }
    }
}
