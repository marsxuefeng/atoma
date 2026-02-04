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
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.BARRIER_NAMESPACE;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Collections.emptyList;

/**
 *
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
@AutoService({CommandHandler.class})
@HandlesCommand(CyclicBarrierCommand.Break.class)
public class BreakCommandHandler extends MongoCommandHandler<CyclicBarrierCommand.Break, Void> {
  @Override
  protected Void execute(CyclicBarrierCommand.Break command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, BARRIER_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(
              and(eq("_id", context.getResourceId()), eq("generation", command.generation())),
              combine(set("is_broken", true), set("participants", emptyList())));
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
