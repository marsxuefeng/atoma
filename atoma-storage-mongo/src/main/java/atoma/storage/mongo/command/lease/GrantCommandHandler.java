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

package atoma.storage.mongo.command.lease;

import atoma.api.AtomaStateException;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LeaseCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.function.Function;

@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(LeaseCommand.Grant.class)
public class GrantCommandHandler
    extends MongoCommandHandler<LeaseCommand.Grant, LeaseCommand.GrantResult> {
  @Override
  protected LeaseCommand.GrantResult execute(
      LeaseCommand.Grant command, MongoCommandHandlerContext context) {
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.LEASE);

    String leaseId = command.id();

    Function<ClientSession, LeaseCommand.GrantResult> cmdBlock =
        (session) -> {
          Instant newExpireTime =
              Instant.now(Clock.systemUTC()).plusMillis(command.ttl().toMillis());

          // Query to find an existing lease that is either expired or does not exist
          Document query = new Document("_id", leaseId);

          // Updates to apply
          Document update =
              new Document("$set", new Document("expire_time", newExpireTime))
                  .append(
                      "$setOnInsert", new Document("create_time", Instant.now(Clock.systemUTC())))
                  .append("$inc", new Document("version", 1L));

          FindOneAndUpdateOptions options =
              new FindOneAndUpdateOptions()
                  .upsert(true) // Insert a new document if no document matches the query
                  .returnDocument(ReturnDocument.AFTER); // Return the document after update

          Document updatedDocument = collection.findOneAndUpdate(query, update, options);

          if (updatedDocument != null) {
            String id = updatedDocument.getString("_id");
            long expireTimeMillis = updatedDocument.get("expire_time", Date.class).getTime();
            return new LeaseCommand.GrantResult(true, id, Instant.ofEpochMilli(expireTimeMillis));
          } else {
            // This should ideally not happen with upsert(true), but handle it just in case
            // For now, returning null implies failure to grant. Depending on business logic,
            // this might need to throw an exception or return a specific error result.
            return new LeaseCommand.GrantResult(false, command.id(), null);
          }
        };

    try {
      return this.newCommandExecutor(context.getClient())
          .withoutTxn()
          .execute(cmdBlock)
          .getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}