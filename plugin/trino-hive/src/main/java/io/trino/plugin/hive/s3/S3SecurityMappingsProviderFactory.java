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

import java.util.List;

public class S3SecurityMappingsProviderFactory
{
    private static final Logger log = Logger.get(S3SecurityMappingsProviderFactory.class);

    private S3SecurityMappingsProviderFactory()
    {}

    private static long getNumberOfMatchingProviders(S3SecurityMappingConfig config)
    {
        List<S3SecurityMappingsProvider> providers = List.of(new FileBasedS3SecurityMappingsProvider(config), new UriBasedS3SecurityMappingsProvider(config));
        return providers.stream().filter(S3SecurityMappingsProvider::checkPreconditions).count();
    }

    public static S3SecurityMappingsProvider createMappingsProvider(S3SecurityMappingConfig config)
    {
        List<S3SecurityMappingsProvider> providers = List.of(new FileBasedS3SecurityMappingsProvider(config), new UriBasedS3SecurityMappingsProvider(config));
        long matching = getNumberOfMatchingProviders(config);
        if (matching == 0) {
            throw new IllegalArgumentException("No provider available for current hive.s3.security-mapping configuration. " +
                    "Try to set either hive.s3.security-mapping.config-file or hive.s3.security-mapping.config-uri.");
        }
        else if (matching > 1) {
            throw new IllegalArgumentException("Ambiguous provider configuration in hive.s3.security-mapping. " +
                    "Please set either hive.s3.security-mapping.config-file or hive.s3.security-mapping.config-uri.");
        }
        else {
            return providers.stream().filter(S3SecurityMappingsProvider::checkPreconditions).findFirst().get();
        }
    }

    public static boolean isProviderAvailable(S3SecurityMappingConfig config)
    {
        long matching = getNumberOfMatchingProviders(config);
        if (matching > 1L) {
            log.warn("Ambiguous provider configuration in hive.s3.security-mapping. " +
                    "Please set either hive.s3.security-mapping.config-file or hive.s3.security-mapping.config-uri.");
        }
        return matching == 1L;
    }
}
