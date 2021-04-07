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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import static java.lang.String.format;

public class UriBasedS3SecurityMappingsProvider
        extends S3SecurityMappingsProvider
{

    public UriBasedS3SecurityMappingsProvider(S3SecurityMappingConfig config)
    {
        super(config);
    }

    @Override
    protected String getRawJSONString()
    {
        String urlString = config.getConfigUri().orElseThrow(() -> new IllegalArgumentException("hive.s3.security-mapping.config-uri file is not set"));
        try {
            URL url = new URL(urlString);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            int status = con.getResponseCode();
            if (200 <= status && status <= 299) {
                try (BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()))) {
                    String inputLine;
                    StringBuilder content = new StringBuilder();
                    while ((inputLine = in.readLine()) != null) {
                        content.append(inputLine);
                    }
                    return content.toString();
                }
            }
            throw new IllegalStateException(format("Request to '%s' returned unexpected status code: '%d'", urlString, status));
        }
        catch (MalformedURLException ex) {
            throw new IllegalArgumentException("hive.s3.security-mapping.config-uri is not a valid URL");
        }
        catch (IOException ex) {
            throw new IllegalStateException(format("Error while sending get request to '%s': '%s'", urlString, ex.getMessage()));
        }
    }

    @Override
    public boolean checkPreconditions()
    {
        return config.getConfigUri().isPresent();
    }
}
