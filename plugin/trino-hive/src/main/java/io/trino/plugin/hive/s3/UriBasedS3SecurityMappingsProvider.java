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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;

import java.net.URI;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.lang.String.format;

public class UriBasedS3SecurityMappingsProvider
        extends S3SecurityMappingsProvider
{
    private final URI configUri;
    private HttpClient httpClient;

    public UriBasedS3SecurityMappingsProvider(S3SecurityMappingConfig config, @ForS3SecurityMapping HttpClient httpClient)
    {
        super(config);
        this.configUri = config.getConfigUri().map(URI::create).orElse(null);
        this.httpClient = httpClient;
    }

    @Override
    protected String getRawJsonString()
    {
        if (this.configUri == null) {
            throw new IllegalArgumentException("hive.s3.security-mapping.config-uri file is not set");
        }
        Request request = prepareGet().setUri(this.configUri).build();
        StringResponseHandler.StringResponse response;
        try {
            response = httpClient.execute(request, createStringResponseHandler());
            int status = response.getStatusCode();
            if (200 <= status && status <= 299) {
                return response.getBody();
            }
            throw new IllegalStateException(format("Request to '%s' returned unexpected status code: '%d'", this.configUri, status));
        }
        catch (RuntimeException ex) {
            throw new IllegalStateException(format("Error while sending get request to '%s'", this.configUri), ex);
        }
    }

    @Override
    public boolean checkPreconditions()
    {
        return configUri != null;
    }
}
