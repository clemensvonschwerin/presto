package io.trino.plugin.hive.s3;

import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static java.lang.String.format;

public class FileBasedS3SecurityMappingsProvider
        extends S3SecurityMappingsProvider
{
    private static final Logger log = Logger.get(FileBasedS3SecurityMappingsProvider.class);

    public FileBasedS3SecurityMappingsProvider(S3SecurityMappingConfig config)
    {
        super(config);
    }

    @Override
    public String getRawJSONString()
    {

        File configFile = this.config.getConfigFile().orElseThrow(() -> new IllegalArgumentException("hive.s3.security-mapping.config-file is not set"));
        log.info("Retrieving config from file %s", configFile);
        try {
            return Files.readString(configFile.toPath());
        }
        catch (IOException ex) {
            throw new IllegalStateException(format("Could not read file '%s', cause: '%s'", configFile.toPath(), ex.getMessage()));
        }
    }

    @Override
    public boolean checkPreconditions()
    {
        return this.config.getConfigFile().isPresent();
    }
}
