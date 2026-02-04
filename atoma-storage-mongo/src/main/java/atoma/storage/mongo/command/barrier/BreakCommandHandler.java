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

package atoma.storage.mongo.command.barrier;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CyclicBarrierCommand;
import atoma.api.coordination.command.HandlesCommand;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.google.errorprone.annotations.Keep;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.BARRIER;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Collections.emptyList;

/**
 *
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <pre>{@code
 * [
 *   {
 *     "_id": "BARRIER-TC-028",
 *     "generation": 17,
 *     "is_broken": false,
 *     "participants": [],
 *     "parties": 2,
 *     "version": NumberLong(1)
 *   }
 * ]
 * }</pre>
 */
@Keep
@AutoService({CommandHandler.class})
@HandlesCommand(CyclicBarrierCommand.Break.class)
public class BreakCommandHandler extends MongoCommandHandler<CyclicBarrierCommand.Break, Void> {
  @Override
  protected Void execute(CyclicBarrierCommand.Break command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, BARRIER);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(
              and(eq("_id", context.getResourceId()), eq("generation", command.generation())),
              combine(
                  set("is_broken", true), set("participants", emptyList()), inc("version", 1L)));
          return null;
        };

    Result<Void> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .withoutCausallyConsistent()
            .retryOnCode(WRITE_CONFLICT)
            .execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}