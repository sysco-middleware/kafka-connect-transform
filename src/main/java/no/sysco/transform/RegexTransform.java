package no.sysco.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

public abstract class RegexTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private RegexTransformConfig config;

    public R apply(final R record) {
        // find a field in R based on the name (like in Timestamp transform)
        // DONE call the replaceAll (or replace first) based on the config
        // DONE put the new value in the record instead of the original one
        // DONE return record
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(final R record) {
        if(config.getField().isEmpty()){
            //apply regex on the entire record, meaning record is a string
            Object value = operatingValue(record);
            return newRecord(record, applyRegexTransformation(value));
        } else {
            //apply regex on a particular field in the record
            final Map<String, Object> value = convertToMap(operatingValue(record));
            final HashMap<String, Object> updatedValue = new HashMap<>(value);
            updatedValue.put(config.getField(), applyRegexTransformation(value.get(config.getField())));
            return newRecord(record, updatedValue);
        }
    }

    private R applyWithSchema(final R record) {
        if(config.getField().isEmpty()){
            //apply regex on the entire record, meaning record is a string
            Object value = operatingValue(record);
            return newRecord(record, applyRegexTransformation(value));
        } else {
            //apply regex on a particular field in the record
            final Schema schema = operatingSchema(record);
            final Struct value = convertToStruct(operatingValue(record));
            final Struct updatedValue = new Struct(value.schema());
            for (Field field : schema.fields()) {
                if (field.name().equals(config.getField())) {
                    Object origFieldValue = value.get(field);
                    updatedValue.put(field, applyRegexTransformation(origFieldValue));
                } else {
                    updatedValue.put(field, value.get(field));
                }
            }
            return newRecord(record, updatedValue);
        }
    }

    public ConfigDef config() {
        return RegexTransformConfig.CONFIG_DEF;
    }

    public void close() {
    }

    public void configure(final Map<String, ?> parsedConfig) {
        this.config = new RegexTransformConfig(parsedConfig);
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(final R record);

    protected abstract R newRecord(R record, Object updatedValue);

    private Object applyRegexTransformation(final Object origObject) {
        if (origObject == null) {
            return null;
        }

        if ( !(origObject instanceof String) ) {
            throw new DataException("RegexTransform cannot apply regular expression transformation on "
                                        + origObject.getClass() + " objects, only String.");
        }

        String str = (String)origObject;
        if(RegexTransformConfig.REPLACEMENT_METHOD_ALL.equals(config.getReplacementMethod())){
            return str.replaceAll(config.getRegex(), config.getReplacement());
        } else {
            return str.replaceFirst(config.getRegex(), config.getReplacement());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertToMap(Object value) {
        if (!(value instanceof Map)) {
            throw new DataException("Only Map objects supported in absence of schema for regex transformation, found: " + getClassName(value));
        }
        return (Map<String, Object>) value;
    }

    private Struct convertToStruct(Object value) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for regex transformation, found: " + getClassName(value));
        }
        return (Struct) value;
    }

    private String getClassName(final Object value) {
        return value == null ? "null" : value.getClass().getName();
    }

    public static class Key<R extends ConnectRecord<R>> extends RegexTransform<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.key();
        }

        @Override
        protected R newRecord(final R record, final Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends RegexTransform<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.value();
        }

        @Override
        protected R newRecord(final R record, final Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }
}
