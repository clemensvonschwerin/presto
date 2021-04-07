package io.trino.plugin.hive.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;

import java.util.function.Supplier;

import static java.lang.String.format;

public abstract class S3SecurityMappingsProvider
        implements Supplier<S3SecurityMappings>
{
    protected final S3SecurityMappingConfig config;

    public S3SecurityMappingsProvider(S3SecurityMappingConfig config)
    {
        this.config = config;
    }

    protected S3SecurityMappings parse(String jsonString)
    {
        try {
            ObjectMapper mapper = new ObjectMapperProvider().get()
                    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
            JsonNode node = mapper.readTree(jsonString);
            JsonNode mappingsNode = node.at(config.getJSONPointer());
            return mapper.treeToValue(mappingsNode, S3SecurityMappings.class);
        }
        catch (JsonProcessingException ex) {
            throw new IllegalArgumentException(format("Could not parse json input string '%s' as s3 security mappings", jsonString));
        }
    }

    protected abstract String getRawJSONString();

    public abstract boolean checkPreconditions();

    @Override
    public S3SecurityMappings get()
    {
        return parse(getRawJSONString());
    }
}
