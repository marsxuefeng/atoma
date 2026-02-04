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
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.BARRIER;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.emptyList;

/**
 * Handles the reset operation for a distributed {@code CyclicBarrier}.
 *
 * <p>This command "breaks" the current generation of the barrier (if any threads are waiting) by
 * incrementing the generation ID and setting an {@code is_broken} flag. It uses an optimistic lock
 * on the {@code version} field to ensure the reset is applied to the expected state, preventing
 * race conditions.
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
@HandlesCommand(CyclicBarrierCommand.Reset.class)
@AutoService({CommandHandler.class})
public class ResetCommandHandler
    extends MongoCommandHandler<CyclicBarrierCommand.Reset, CyclicBarrierCommand.GetStateResult> {

  /**
   *
   *
   * <h3>Fake-code for await logical</h3>
   *
   * <pre>{@code
   * if ( barrier exists ) : {
   *     $$ROOT
   *     generation += 1
   *     is_broken = false
   *     participants = []
   *     parties = <input parties>
   *     version += 1
   *     return
   * }else{
   *     $$ROOT
   *     parties = <input parties>
   *     generation = 1
   *     is_broken = false
   *     participants = []
   *     version = 1
   *     return
   * }
   * }</pre>
   *
   * @param command reset command
   * @return The latest value for current state
   */
  private List<Bson> buildAggregationPipeline(CyclicBarrierCommand.Reset command) {
    return List.of(
        replaceRoot(
            new Document(
                "$cond",
                Arrays.asList(

                    // -------- if: barrier exists --------
                    new Document(
                        "$ne", Arrays.asList(new Document("$type", "$parties"), "missing")),

                    // -------- then --------
                    new Document(
                        "$mergeObjects",
                        Arrays.asList(
                            "$$ROOT",
                            new Document(
                                    "generation",
                                    new Document("$add", Arrays.asList("$generation", 1L)))
                                .append("is_broken", false)
                                .append("parties", command.parties())
                                .append("participants", emptyList())
                                .append("version", new Document("$add", List.of("$version", 1L))))),

                    // -------- else --------
                    new Document(
                        "$mergeObjects",
                        Arrays.asList(
                            "$$ROOT",
                            new Document("generation", 1L)
                                .append("is_broken", false)
                                .append("participants", emptyList())
                                .append("version", 1L)))))));
  }

  /**
   * Executes the atomic reset command.
   *
   * @param command The command containing the expected version for optimistic locking.
   * @param context The context for command execution.
   * @return latest state data.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public CyclicBarrierCommand.GetStateResult execute(
      CyclicBarrierCommand.Reset command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, BARRIER);
    List<Bson> pipeline = buildAggregationPipeline(command);
    Function<ClientSession, CyclicBarrierCommand.GetStateResult> cmdBlock =
        session -> {
          Document barrierDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (barrierDoc == null) {
            throw new AtomaStateException(
                "Failed to find or reset barrier. Because barrier does not existed in MongoDB Database.");
          }

          return new CyclicBarrierCommand.GetStateResult(
              command.parties(),
              barrierDoc.getList("participants", Document.class).size(),
              false,
              barrierDoc.getLong("generation"));
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
