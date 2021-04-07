package io.trino.plugin.hive.s3;

import io.airlift.log.Logger;

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

    private static final Logger log = Logger.get(UriBasedS3SecurityMappingsProvider.class);

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
