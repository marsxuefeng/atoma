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
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.setOnInsert;
import static java.util.Collections.emptyList;

/**
 * Handles fetching the current state of a distributed {@code CyclicBarrier}.
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
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(CyclicBarrierCommand.GetState.class)
public class GetStateCommandHandler
    extends MongoCommandHandler<
        CyclicBarrierCommand.GetState, CyclicBarrierCommand.GetStateResult> {

  /**
   * Executes the command to fetch the current state of the barrier.
   *
   * @param command The {@link CyclicBarrierCommand.GetState} command.
   * @param context The context for command execution.
   * @return A {@link CyclicBarrierCommand.GetStateResult} containing the barrier's current state.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public CyclicBarrierCommand.GetStateResult execute(
      CyclicBarrierCommand.GetState command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, AtomaCollectionNamespace.BARRIER);

    Function<ClientSession, CyclicBarrierCommand.GetStateResult> cmdBlock =
        session -> {
          Document doc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  combine(
                      setOnInsert("parties", command.parties()),
                      setOnInsert("generation", 1L),
                      setOnInsert("is_broken", false),
                      setOnInsert("participants", emptyList()),
                      setOnInsert("version", 1L)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (doc == null) {
            return new CyclicBarrierCommand.GetStateResult(0, 0, false, 0L);
          }

          int parties = doc.getInteger("parties", 0);
          boolean isBroken = doc.getBoolean("is_broken", false);
          long generation = doc.getLong("generation");
          var participants = doc.getList("participants", Document.class);
          return new CyclicBarrierCommand.GetStateResult(
              parties, participants == null ? 0 : participants.size(), isBroken, generation);
        };

    Result<CyclicBarrierCommand.GetStateResult> result =
        this.newCommandExecutor(client).withoutTxn().withoutCausallyConsistent().execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}