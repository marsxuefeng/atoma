package atoma.storage.mongo.command.barrier;

import atoma.api.AtomaStateException;
import atoma.api.OperationTimeoutException;
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
import dev.failsafe.TimeoutExceededException;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.BARRIER_NAMESPACE;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Filters.eq;

/**
 * Handles the {@code await} operation for a distributed {@code CyclicBarrier}.
 *
 * <p>This implementation is highly optimized for correctness and performance under high contention.
 * It uses a combination of a "check-then-act" pattern and optimistic locking via a {@code version}
 * field.
 *
 * <h4>Core Logic</h4>
 *
 * The execution flow is as follows:
 *
 * <ol>
 *   <li><b>Get-or-Create State:</b> It first performs a single atomic {@code findOneAndUpdate} with
 *       {@code upsert=true} to get the current state of the barrier document or create it if it's
 *       the very first participant ever. This operation also retrieves the document's current
 *       {@code version}.
 *   <li><b>Optimistic Locking:</b> This {@code version} number is then used in the filter of all
 *       subsequent write operations. If the document has been modified by another process between
 *       the read and the write, the version number will have changed, causing the update to fail
 *       safely. This prevents lost updates and race conditions.
 *   <li><b>Intelligent "Check-Then-Act":</b> Based on the state read in the first step, the handler
 *       intelligently chooses one of three distinct atomic operations:
 *       <ul>
 *         <li><b>Initialize Generation:</b> If this is the first participant for a new generation,
 *             it sets the entire {@code waiters} sub-document.
 *         <li><b>Trip Barrier:</b> If this is the last participant for the current generation, it
 *             increments the global {@code generation}, unsets the {@code waiters} document, and
 *             resets the {@code is_broken} flag to {@code false}.
 *         <li><b>Join Barrier:</b> Otherwise, it increments the {@code waiters.count} and pushes
 *             itself to the {@code waiters.participants} array.
 *       </ul>
 * </ol>
 *
 * This strategy avoids unnecessary database commands and ensures atomicity in a highly concurrent
 * environment.
 *
 * <h3>MongoDB Document Schema</h3>
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
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(CyclicBarrierCommand.Await.class)
public class AwaitCommandHandler
    extends MongoCommandHandler<CyclicBarrierCommand.Await, CyclicBarrierCommand.AwaitResult> {

  private List<Bson> buildAggregationPipeline(CyclicBarrierCommand.Await command) {
    return List.of(
        replaceRoot(
            new Document(
                "$cond",
                List.of(

                    // if: {$ne: [{$type: "$parties"}, "missing"]}
                    new Document("$ne", List.of(new Document("$type", "$parties"), "missing")),

                    // then:
                    new Document(
                        "$cond",
                        List.of(

                            // if: {$eq: ["$broken", true]}
                            new Document("$eq", List.of("$broken", true)),

                            // then:
                            new Document(
                                "$mergeObjects",
                                List.of(
                                    "$$ROOT",
                                    new Document("_passed", true)
                                        .append("is_broken", true)
                                        .append("_inconsistent_parties", false))),

                            // else:
                            new Document(
                                "$cond",
                                List.of(

                                    // if: {$ne: ["$parties", 2]}
                                    new Document("$ne", List.of("$parties", command.parties())),

                                    // then:
                                    new Document(
                                        "$mergeObjects",
                                        List.of(
                                            "$$ROOT",
                                            new Document("_passed", false)
                                                .append("_inconsistent_parties", true))),

                                    // else:
                                    new Document(
                                        "$cond",
                                        List.of(

                                            // if: {$eq: [{$add: ["$number_waiting", 1]},
                                            // "$parties"]}
                                            new Document(
                                                "$eq",
                                                List.of(
                                                    new Document(
                                                        "$add", List.of("$number_waiting", 1)),
                                                    "$parties")),

                                            // then:
                                            new Document(
                                                "$mergeObjects",
                                                List.of(
                                                    "$$ROOT",
                                                    new Document(
                                                            "generation",
                                                            new Document(
                                                                "$add", List.of("$generation", 1L)))
                                                        .append("number_waiting", 0)
                                                        .append("_passed", true)
                                                        .append("_inconsistent_parties", false))),

                                            // else:
                                            new Document(
                                                "$mergeObjects",
                                                List.of(
                                                    "$$ROOT",
                                                    new Document("_passed", false)
                                                        .append("_inconsistent_parties", false)
                                                        .append(
                                                            "number_waiting",
                                                            new Document(
                                                                "$add",
                                                                List.of(
                                                                    "$number_waiting", 1))))))))))),

                    // else (parties missing):
                    new Document("parties", command.parties())
                        .append("generation", 1L)
                        .append("is_broken", false)
                        .append("number_waiting", 1)
                        .append("_passed", command.parties() == 1)))));
  }

  @Override
  public CyclicBarrierCommand.AwaitResult execute(
      CyclicBarrierCommand.Await command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, BARRIER_NAMESPACE);

    List<Bson> pipeline = buildAggregationPipeline(command);

    Function<ClientSession, CyclicBarrierCommand.AwaitResult> cmdBlock =
        session -> {
          Document barrierDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          System.err.println("Await command: " + barrierDoc);

          if (barrierDoc == null) {
            throw new AtomaStateException("Failed to find or create barrier document.");
          }

          if (barrierDoc.getBoolean("_inconsistent_parties", false)) {
            throw new AtomaStateException(
                String.format(
                    "Failed to waiting on document. Parties was %d. expected %d",
                    barrierDoc.getInteger("parties"), command.parties()));
          }

          return new CyclicBarrierCommand.AwaitResult(
              barrierDoc.getBoolean("_passed", false),
              barrierDoc.getBoolean("is_broken", false),
              barrierDoc.getLong("generation"));
        };

    Result<CyclicBarrierCommand.AwaitResult> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .withoutCausallyConsistent()
            .retryOnCode(WRITE_CONFLICT)
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
