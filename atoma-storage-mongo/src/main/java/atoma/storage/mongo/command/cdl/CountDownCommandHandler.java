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

package atoma.storage.mongo.command.cdl;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CountDownLatchCommand;
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

import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.COUNTDOWN_LATCH;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Filters.eq;

/**
 * Handles the {@code countDown} operation for a distributed {@code CountDownLatch}.
 *
 * <p>This handler uses a single, atomic {@code updateOne} operation with the {@code $inc} operator
 * to decrement the {@code count} field. A crucial part of the query filter is the {@code
 * gt("count", 0)} condition. This ensures that the count never drops below zero, and that calling
 * {@code countDown()} on a completed latch (where count is 0) becomes a safe, silent no-op, which
 * is consistent with the behavior of {@link java.util.concurrent.CountDownLatch}.
 *
 * <h3>MongoDB Document Schema for CountDownLatch</h3>
 *
 * <pre>{@code
 * {
 *   "_id": "latch-resource-id",
 *   "count": 3,
 *   "version": NumberLong(1),
 *   "_update_flag:": true
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@HandlesCommand(CountDownLatchCommand.CountDown.class)
@AutoService({CommandHandler.class})
public class CountDownCommandHandler
    extends MongoCommandHandler<CountDownLatchCommand.CountDown, Void> {

  /**
   *
   *
   * <h3>Fake-code for count-down</h3>
   *
   * <pre>{@code
   * if ( count-down-latch existed ) {
   *     if ( $count > 0 ) : {
   *         $$ROOT
   *         count -= 1
   *         version += 1
   *         _update_flag = true
   *     }else{
   *         $$ROOT
   *         _update_flag = false
   *         version += 1
   *     }
   * } else {
   *     version = 1
   *     count = 1
   *     _update_flag = true
   *     count = 3
   * }
   * }</pre>
   *
   * @param command The {@link CountDownLatchCommand.CountDown} command.
   * @return A {@link List} of {@link Bson} stages for the {@code findOneAndUpdate} operation.
   */
  private List<Bson> buildAggregationPipeline(CountDownLatchCommand.CountDown command) {
    return List.of(
        replaceRoot(
            new Document(
                "$cond",
                List.of(

                    // ===== if ( count-down-latch existed ) =====
                    new Document("$ne", List.of(new Document("$type", "$count"), "missing")),

                    // ===== THEN: latch existed =====
                    new Document(
                        "$cond",
                        List.of(

                            // if ( $count > 0 )
                            new Document("$gt", List.of("$count", 0)),

                            // ---- count > 0 ----
                            new Document(
                                "$mergeObjects",
                                List.of(
                                    "$$ROOT",
                                    new Document(
                                        "count", new Document("$subtract", List.of("$count", 1))),
                                    new Document(
                                        "version", new Document("$add", List.of("$version", 1L))),
                                    new Document("_update_flag", true))),

                            // ---- count <= 0 ----
                            new Document(
                                "$mergeObjects",
                                List.of(
                                    "$$ROOT",
                                    new Document(
                                        "version", new Document("$add", List.of("$version", 1L))),
                                    new Document("_update_flag", false))))),

                    // ===== ELSE: latch not existed (init) =====
                    new Document(
                        "$mergeObjects",
                        List.of(
                            "$$ROOT",
                            new Document("version", 1L),
                            new Document("count", command.count() - 1),
                            new Document("_update_flag", true)))))));
  }

  /**
   * Executes the atomic decrement command.
   *
   * @param command The {@link CountDownLatchCommand.CountDown} command.
   * @param context The context for command execution.
   * @return Void on success.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public Void execute(CountDownLatchCommand.CountDown command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, COUNTDOWN_LATCH);

    List<Bson> pipeline = buildAggregationPipeline(command);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          Document countDownLatchDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (countDownLatchDoc == null) {
            throw new IllegalStateException(
                "Failed to count-down. the count-down-latch does not exist");
          }
          return null;
        };

    Result<Void> result = this.newCommandExecutor(client).withoutTxn().execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
