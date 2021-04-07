package io.trino.plugin.hive.s3;

import io.trino.spi.security.ConnectorIdentity;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static org.testng.AssertJUnit.assertTrue;

public class TestS3SecurityMappingsProvider
{
    @Test
    public void testParse()
    {
        S3SecurityMappingConfig conf = new S3SecurityMappingConfig()
                .setJSONPointer("/data");

        StubS3SecurityMappingsProvider provider = new StubS3SecurityMappingsProvider(conf);
        S3SecurityMappings mappings =
                provider.parse("{\"data\": {\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}, \"time\": \"30s\"}");

        Optional<S3SecurityMapping> mapping = mappings.getMapping(ConnectorIdentity.ofUser("test"), URI.create("http://trino"));
        assertTrue(mapping.isPresent());
    }

    public static class StubS3SecurityMappingsProvider
            extends S3SecurityMappingsProvider
    {

        public StubS3SecurityMappingsProvider(S3SecurityMappingConfig config)
        {
            super(config);
        }

        @Override
        protected String getRawJSONString()
        {
            return null;
        }

        @Override
        public boolean checkPreconditions()
        {
            return false;
        }
    }
}
