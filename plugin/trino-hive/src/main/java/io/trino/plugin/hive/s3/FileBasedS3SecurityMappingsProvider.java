/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
