package atoma.test.mutex;

import atoma.api.coordination.command.SemaphoreCommand;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.LEASE_NAMESPACE;
import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.unwind;
import static java.util.Arrays.asList;

public class TmpTest {
  public static void printScript(List<? extends Bson> pipeline) {
    var text =
        String.format(
            " [ %s ] ",
            pipeline.stream()
                .map(t -> t.toBsonDocument().toJson())
                .collect(Collectors.joining(",")));

    System.out.printf("pipeline script %s %n", text);
  }

  public static void main(String[] args) {

    SemaphoreCommand.Acquire command =
        new SemaphoreCommand.Acquire(1, "abc", -1, TimeUnit.SECONDS, 1);

    var owner = new Document().append("holder", "1").append("lease", "1");

    final List<Bson> pipeline =
            asList(
                    unwind("$participants"),
                    lookup(LEASE_NAMESPACE, "participants.lease", "_id", "lease_doc"),
                    match(
                            new Document(
                                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
                    group("$_id", addToSet("dead_participant_leases", "$participants.lease")));

    printScript(pipeline);
  }
}
