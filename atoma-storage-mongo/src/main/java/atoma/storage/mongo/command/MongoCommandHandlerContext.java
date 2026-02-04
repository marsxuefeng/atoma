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

package atoma.storage.mongo.command;

import atoma.api.coordination.Resource;
import atoma.api.coordination.command.CommandHandlerContext;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.Optional;

public class MongoCommandHandlerContext implements CommandHandlerContext {
  private final MongoClient client;
  private final String resourceId;
  private final MongoDatabase mongoDatabase;

  public MongoCommandHandlerContext(
      MongoClient client, MongoDatabase mongoDatabase, String resourceId) {
    this.client = client;
    this.mongoDatabase = mongoDatabase;
    this.resourceId = resourceId;
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  @Override
  public Optional<Resource> getCurrentResource() {
    // This could be implemented to pre-fetch the resource if needed.
    return Optional.empty();
  }

  public MongoClient getClient() {
    return client;
  }

  public MongoDatabase getMongoDatabase() {
    return mongoDatabase;
  }
}