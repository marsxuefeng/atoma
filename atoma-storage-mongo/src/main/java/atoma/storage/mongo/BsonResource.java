/*
 * Copyright 2025 XueFeng Ma
 *
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

package atoma.storage.mongo;

import atoma.api.coordination.Resource;
import org.bson.Document;

import java.util.Map;

public final class BsonResource implements Resource {

  private final Document bson;

  public BsonResource(Document bson) {
    this.bson = bson;
  }

  @Override
  public long getVersion() {
    return this.bson.getLong("version");
  }

  @Override
  public String getId() {
    return this.bson.getString("_id");
  }

  @Override
  public Map<String, Object> getData() {
    return this.bson;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T get(String key) {
    return (T) this.bson.get(key);
  }

  @Override
  public <T> T get(String key, T defaultValue) {
    T r;
    if ((r = get(key)) != null) return r;
    return defaultValue;
  }
}