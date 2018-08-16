package no.sysco.transform;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RegexTransformConfig extends AbstractConfig {

    public static final String FIELD_CONFIG = "field";
    private static final String FIELD_DEFAULT = "";

    public static final String REGEX_CONFIG = "field.regex";

    public static final String REPLACEMENT_CONFIG = "field.replacement";

    public static final String REPLACEMENT_METHOD_CONFIG = "field.replacement.method";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FIELD_CONFIG,
                ConfigDef.Type.STRING,
                FIELD_DEFAULT,
                ConfigDef.Importance.HIGH,
                "The field containing the string to apply replacement, or empty if the entire value is a string.")
        .define(REGEX_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Regular expression to use for matching.")
        .define(REPLACEMENT_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Replacement string.")
        .define(REPLACEMENT_METHOD_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.MEDIUM,
                "Replacement method tells whether to replace all the occurrences of the matched string or only the first one. " +
                    "Valid values are: ALL, FIRST. Default value: ALL.");

    public static final String REPLACEMENT_METHOD_ALL = "ALL";
    public static final String REPLACEMENT_METHOD_FIRST = "FIRST";

    public static final Set<String> VALID_REPLACEMENT_METHODS = new HashSet<>(Arrays.asList(REPLACEMENT_METHOD_ALL, REPLACEMENT_METHOD_FIRST));

    private final String field;
    private final String regex;
    private final String replacement;
    private final String replacementMethod;

    public RegexTransformConfig(final Map<?, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.field = getString(FIELD_CONFIG);
        this.regex = getString(REGEX_CONFIG);
        this.replacement = getString(REPLACEMENT_CONFIG);
        this.replacementMethod = getString(REPLACEMENT_METHOD_CONFIG);

        if (regex.trim().isEmpty()) {
            throw new ConfigException("RegexTransform requires regex option to be specified");
        }

        if (!VALID_REPLACEMENT_METHODS.contains(replacementMethod)) {
            throw new ConfigException("Unknown replacement method in RegexTransform: " + replacementMethod + ". Valid values are "
                                          + Utils.join(VALID_REPLACEMENT_METHODS, ", ") + ".");
        }
    }

    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    public String getField() {
        return field;
    }

    public String getRegex() {
        return regex;
    }

    public String getReplacement() {
        return replacement;
    }

    public String getReplacementMethod() {
        return replacementMethod;
    }
}
