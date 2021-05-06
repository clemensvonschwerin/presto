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

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.plugin.base.util.JsonUtils;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class S3SecurityMappingsProvider
        implements Supplier<S3SecurityMappings>
{
    protected final String jsonPointer;

    protected S3SecurityMappingsProvider(S3SecurityMappingConfig config)
    {
        this.jsonPointer = requireNonNull(config.getJsonPointer());
    }

    protected S3SecurityMappings parse(String jsonString)
    {
        JsonNode node = JsonUtils.parseJson(jsonString);
        JsonNode mappingsNode = node.at(this.jsonPointer);
        return JsonUtils.jsonTreeToValue(mappingsNode, S3SecurityMappings.class);
    }

    protected abstract String getRawJsonString();

    @Override
    public S3SecurityMappings get()
    {
        return parse(getRawJsonString());
    }
}
