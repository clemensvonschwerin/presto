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

import com.sun.net.httpserver.HttpServer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;

import static org.testng.AssertJUnit.assertEquals;

public class TestUriBasedS3SecurityMappingsProvider
{

    @Test
    public void testGetRawJSON()
    {
        try (FakeServer server = new FakeServer()) {
            S3SecurityMappingConfig conf = new S3SecurityMappingConfig()
                    .setConfigUri("http://" + server.address.getHostString() + ":" + server.address.getPort() + "/api/endpoint");
            UriBasedS3SecurityMappingsProvider provider =
                    new UriBasedS3SecurityMappingsProvider(conf);
            String result = provider.getRawJSONString();
            assertEquals("{\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}", result);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static class FakeServer
            implements AutoCloseable
    {
        HttpServer httpServer;
        InetSocketAddress address;

        FakeServer()
                throws IOException
        {
            address = new InetSocketAddress(1234);
            httpServer = HttpServer.create(address, 0);
            httpServer.createContext("/api/endpoint", exchange -> {
                byte[] response = "{\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}".getBytes();
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
            });
            httpServer.start();
        }

        @Override
        public void close()
                throws Exception
        {
            httpServer.stop(0);
        }
    }
}
