package atoma.storage.mongo.command.barrier;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CyclicBarrierCommand;
import atoma.api.coordination.command.HandlesCommand;
import atoma.storage.mongo.command.CommandFailureException;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.BARRIER_NAMESPACE;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;

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
 * {
 *   "_id": "barrier-resource-id",
 *   "parties": 5,
 *   "generation": NumberLong(1),
 *   "is_broken": false,
 *   "number_waiting": 12,
 *   "_passed": false,
 *   "_inconsistent_parties": false,
 *   "_waited": false,
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@HandlesCommand(CyclicBarrierCommand.Reset.class)
@AutoService({CommandHandler.class})
public class ResetCommandHandler extends MongoCommandHandler<CyclicBarrierCommand.Reset, Void> {

  /**
   * Executes the atomic reset command.
   *
   * @param command The command containing the expected version for optimistic locking.
   * @param context The context for command execution.
   * @return Void on success.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public Void execute(CyclicBarrierCommand.Reset command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, BARRIER_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          Document barrierDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  combine(
                      inc("generation", 1L), // Break current waiters
                      set("is_broken", false), // Mark as broken
                      set("number_waiting", 0) // Clear all waiters
                      ));

          if (barrierDoc == null) {
            throw new AtomaStateException(
                "Failed to find or reset barrier. Because barrier does not existed in MongoDB Database.");
          }

          if (!barrierDoc.getBoolean("is_broken")) throw new CommandFailureException();

          return null;
        };

    Result<Void> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .withoutCausallyConsistent()
            .retryOnException(CommandFailureException.class)
            .execute(cmdBlock);
    try {
      result.getOrThrow();
      return null;
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
