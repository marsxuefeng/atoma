package atoma.test.mutex;

import atoma.api.coordination.command.SemaphoreCommand;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.LEASE;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.push;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
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
                    project(
                            fields(
                                    include("_id", "available_permits", "version"),
                                    computed("leases_as_array", new Document("$objectToArray", "$leases")))),
                    unwind("$leases_as_array"),
                    lookup(LEASE, "leases_as_array.k", "_id", "lease_doc"),
                    match(
                            new Document(
                                    "$expr", new Document("$eq", List.of(new Document("$size", "$lease_doc"), 0)))),
                    limit(500),
                    group(
                            "$_id",
                            first("version", "$version"),
                            sum("permits_to_return", "$leases_as_array.v"),
                            push("dead_leases_keys", "$leases_as_array.k")));

    printScript(pipeline);
  }
}
