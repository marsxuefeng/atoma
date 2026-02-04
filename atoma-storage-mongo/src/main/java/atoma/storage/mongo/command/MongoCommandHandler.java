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

import atoma.api.coordination.command.Command;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CommandHandlerContext;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.concurrent.ConcurrentHashMap;

import static atoma.storage.mongo.command.CommandExecutor.READ_CONCERN;
import static atoma.storage.mongo.command.CommandExecutor.WRITE_CONCERN;

public abstract class MongoCommandHandler<C extends Command<R>, R> implements CommandHandler<C, R> {
  private static final ConcurrentHashMap<String, MongoCollection<Document>>
      COLLECTION_CONCURRENT_HASH_MAP = new ConcurrentHashMap<>();

  protected MongoCollection<Document> getCollection(
      MongoCommandHandlerContext context, String name) {

    return COLLECTION_CONCURRENT_HASH_MAP.computeIfAbsent(
        name,
        _k ->
            context
                .getMongoDatabase()
                .getCollection(name)
                .withReadConcern(READ_CONCERN)
                .withWriteConcern(WRITE_CONCERN));
  }

  @Override
  public R execute(C command, CommandHandlerContext context) {
    return this.execute(command, (MongoCommandHandlerContext) context);
  }

  protected abstract R execute(C command, MongoCommandHandlerContext context);

  public CommandExecutor<R> newCommandExecutor(MongoClient client) {
    return new CommandExecutor<>(client);
  }
}