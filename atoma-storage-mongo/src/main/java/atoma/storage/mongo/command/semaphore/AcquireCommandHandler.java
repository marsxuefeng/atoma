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

package atoma.storage.mongo.command.semaphore;

import atoma.api.AtomaStateException;
import atoma.api.OperationTimeoutException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.SemaphoreCommand;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import dev.failsafe.TimeoutExceededException;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.SEMAPHORE;
import static atoma.storage.mongo.command.MongoErrorCode.DUPLICATE_KEY;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.emptyMap;

/**
 * Handles the server-side logic for acquiring permits from a distributed semaphore.
 *
 * <p>The logic is designed to be atomic and handles two main scenarios within a single command
 * execution:
 *
 * <ol>
 *   <li><b>Acquisition from Existing Semaphore:</b> It first attempts a {@code findOneAndUpdate}
 *       operation, conditioned on {@code available_permits} being sufficient. If this succeeds, the
 *       permits are acquired atomically.
 *   <li><b>Semaphore Creation (Upsert):</b> If the first attempt fails (e.g., the document does not
 *       exist or has insufficient permits), it then attempts an {@code updateOne} with {@code
 *       upsert=true}. This operation will create and initialize the semaphore only if it doesn't
 *       exist and has enough permits from the start.
 * </ol>
 *
 * The handler returns a result indicating if the acquisition was successful in either case.
 *
 * <h3>MongoDB Document Schema for Semaphore</h3>
 *
 * <pre>{@code
 * {
 *   "_id": "semaphore-resource-id",
 *   "initial_permits": 10,
 *   "available_permits": 4,
 *   "leases": {
 *     "lease-abc": 3,
 *     "lease-xyz": 3
 *   },
 *   "version": NumberLong(1),
 *   "_update_flag:": true
 * }
 * </pre>
 *
 * <ul>
 *   <li><b>_id</b>: The unique ID of the semaphore resource.
 *   <li><b>initial_permits</b>: The total number of permits defined upon creation.
 *   <li><b>available_permits</b>: The current number of available permits.
 *   <li><b>leases</b>: A map tracking the number of permits held by each client lease, crucial for
 *       safe releases and automatic cleanup on lease expiration.
 * </ul>
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(SemaphoreCommand.Acquire.class)
public class AcquireCommandHandler
    extends MongoCommandHandler<SemaphoreCommand.Acquire, SemaphoreCommand.AcquireResult> {

  /**
   * In the MongoDB database system, the {@link MongoCollection#updateOne(Bson, Bson)} API can
   * return a flag indicating whether the data has been updated, but it will not return the updated
   * document version. The {@link MongoCollection#findOneAndUpdate(Bson, List)} API can return the
   * flag indicating the updated version, but it can not to determine whether the document has been
   * successfully updated. Therefore, a redundant field '_update_flag' has been added
   * semaphore-document, which has a very low cost and can help us solve the problems of the two
   * APIs mentioned above
   *
   * <h3>Fake-code for acquire logical</h3>
   *
   * <pre>{@code
   * if ( semaphore existed ) {
   *     if ( <acquire permits>  <=  $available_permits ) {
   *         $$ROOT
   *         available_permits -= <acquire permits>
   *         version += 1
   *         leases.lease-abc: {
   *             $inc: <acquire permits>
   *         }
   *         _update_flag = true
   *         return
   *     }else{
   *         $$ROOT
   *         _update_flag = false
   *         return
   *     }
   * } else {
   *     if ( <acquire permits>  <=  <initial_permits> ) {
   *         version = 1
   *         available_permits = <initial_permits> - <input permits>
   *         initial_permits = <initial_permits>
   *         _update_flag = true
   *         leases.lease-abc= <input permits>
   *         return
   *     }else{
   *         version = 1
   *         available_permits = <input permits>
   *         _update_flag = false
   *         initial_permits = <input permits>
   *         leases.lease-abc = {}
   *         return
   *     }
   * }
   * }</pre>
   *
   * @see MongoCollection#findOneAndUpdate(Bson, List)
   * @see MongoCollection#updateOne(Bson, Bson)
   * @param command acquire command
   * @return A {@link List} of {@link Bson} stages for the {@code findOneAndUpdate} operation.
   */
  private List<Bson> buildAggregationPipeline(SemaphoreCommand.Acquire command) {
    int acquirePermits = command.permits();
    int initialPermits = command.initialPermits();
    return List.of(
        replaceRoot(
            new Document(
                "$cond",
                List.of(

                    // ===== if ( semaphore existed ) =====
                    new Document(
                        "$ne", List.of(new Document("$type", "$available_permits"), "missing")),

                    // ================= existed =================
                    new Document(
                        "$cond",
                        List.of(

                            // if ( acquirePermits <= available_permits )
                            new Document("$lte", List.of(acquirePermits, "$available_permits")),

                            // ---- acquire success ----
                            new Document(
                                "$mergeObjects",
                                List.of(
                                    "$$ROOT",

                                    // available_permits -= acquirePermits
                                    new Document(
                                        "available_permits",
                                        new Document(
                                            "$subtract",
                                            List.of("$available_permits", acquirePermits))),

                                    // version += 1
                                    new Document(
                                        "version", new Document("$add", List.of("$version", 1L))),

                                    // leases.lease-id += acquirePermits
                                    new Document(
                                        "leases",
                                        new Document(
                                            "$mergeObjects",
                                            List.of(
                                                new Document(
                                                    "$ifNull", List.of("$leases", emptyMap())),
                                                new Document(
                                                    command.leaseId(),
                                                    new Document(
                                                        "$add",
                                                        List.of(
                                                            new Document(
                                                                "$ifNull",
                                                                List.of(
                                                                    "$leases." + command.leaseId(),
                                                                    0)),
                                                            acquirePermits)))))),
                                    new Document("_update_flag", true))),

                            // ---- acquire failed ----
                            new Document(
                                "$mergeObjects",
                                List.of("$$ROOT", new Document("_update_flag", false))))),

                    // ================= not existed =================
                    new Document(
                        "$cond",
                        List.of(

                            // if ( acquirePermits <= initialPermits )
                            new Document("$lte", List.of(acquirePermits, initialPermits)),

                            // ---- init & acquire success ----
                            new Document(
                                "$mergeObjects",
                                List.of(
                                    "$$ROOT",
                                    new Document("version", 1L),
                                    new Document("initial_permits", initialPermits),
                                    new Document(
                                        "available_permits", initialPermits - acquirePermits),
                                    new Document(
                                        "leases", new Document(command.leaseId(), acquirePermits)),
                                    new Document("_update_flag", true))),

                            // ---- init but acquire failed ----
                            new Document(
                                "$mergeObjects",
                                List.of(
                                    "$$ROOT",
                                    new Document("version", 1L),
                                    new Document("initial_permits", initialPermits),
                                    new Document("available_permits", initialPermits),
                                    new Document("leases", emptyMap()),
                                    new Document("_update_flag", false)))))))));
  }

  /**
   * Executes the atomic logic to acquire permits from the semaphore.
   *
   * @param command The {@link SemaphoreCommand.Acquire} command, containing the number of permits
   *     requested and caller identification.
   * @param context The context for command execution.
   * @return A {@link SemaphoreCommand.AcquireResult} indicating whether the acquisition was
   *     successful.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public SemaphoreCommand.AcquireResult execute(
      SemaphoreCommand.Acquire command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, SEMAPHORE);

    List<Bson> pipeline = buildAggregationPipeline(command);

    Function<ClientSession, SemaphoreCommand.AcquireResult> cmdBlock =
        session -> {

          // First, try to acquire from an existing semaphore.
          // Condition: available_permits must be sufficient.
          Document semaphoreDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true));

          // The reason for not using the version number as the basis for determining success is
          // that the mongo-filter only sets the _id to be equal to a fixed value.
          // May be two threads reading the same version number at the same time, and only one
          // thread will acquire successfully. However, the updated version number returned is
          // equals
          // (original version number + 1), which can lead to the illusion that both
          // threads will acquire successfully. To solve this problem, a redundant field will be
          // added to the conditional judgment branch in the update pipeline to address this issue
          if (semaphoreDoc != null) {
            return new SemaphoreCommand.AcquireResult(
                semaphoreDoc.getBoolean("_update_flag", false), semaphoreDoc.getLong("version"));
          }
          return new SemaphoreCommand.AcquireResult(false, -1L);
        };

    Result<SemaphoreCommand.AcquireResult> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .withoutCausallyConsistent()
            .retryOnCode(WRITE_CONFLICT)
            .retryOnCode(DUPLICATE_KEY)
            .withTimeout(Duration.of(command.timeout(), command.timeUnit().toChronoUnit()))
            .execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      // Translate Exception
      if (e instanceof TimeoutExceededException timeoutEx) {
        throw new OperationTimeoutException(timeoutEx);
      }
      throw new AtomaStateException(e);
    }
  }
}
