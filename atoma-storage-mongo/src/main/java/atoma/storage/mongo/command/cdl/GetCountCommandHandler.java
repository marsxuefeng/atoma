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
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.COUNTDOWN_LATCH;
import static com.mongodb.client.model.Filters.eq;

/**
 * Handles fetching the current count of a distributed {@code CountDownLatch}.
 *
 * <p>This handler performs a {@code find} operation to retrieve the latch document. If the document
 * does not exist (e.g., it was never created or has been destroyed), it correctly returns a count
 * of 0, as any {@code await()} call on such a latch should pass immediately.
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
@HandlesCommand(CountDownLatchCommand.GetCount.class)
@AutoService({CommandHandler.class})
public class GetCountCommandHandler
    extends MongoCommandHandler<
        CountDownLatchCommand.GetCount, CountDownLatchCommand.GetCountResult> {

  /**
   * Executes the command to fetch the current count.
   *
   * @param command The {@link CountDownLatchCommand.GetCount} command.
   * @param context The context for command execution.
   * @return A {@link CountDownLatchCommand.GetCountResult} containing the current count.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public CountDownLatchCommand.GetCountResult execute(
      CountDownLatchCommand.GetCount command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, COUNTDOWN_LATCH);

    Function<ClientSession, CountDownLatchCommand.GetCountResult> cmdBlock =
        session -> {
          Document doc = collection.find(eq("_id", context.getResourceId())).first();
          if (doc != null && doc.getInteger("count") != null) {
            return new CountDownLatchCommand.GetCountResult(
                doc.getInteger("count"), doc.getLong("version"));
          } else {
            // If doc doesn't exist, count is effectively 0 for any waiting threads.
            return new CountDownLatchCommand.GetCountResult(0, -1L);
          }
        };
    Result<CountDownLatchCommand.GetCountResult> result =
        this.newCommandExecutor(client).withoutCausallyConsistent().withoutTxn().execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
