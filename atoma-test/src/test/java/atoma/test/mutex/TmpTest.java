package atoma.test.mutex;

import atoma.api.coordination.command.SemaphoreCommand;
import com.mongodb.client.model.Aggregates;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    var pipeline =
        List.of(
            Aggregates.replaceRoot(
                new Document(
                    "$cond",
                    Arrays.asList(

                        // ================= if =================
                        new Document(
                            "$and",
                            Arrays.asList(

                                // ---- read_locks missing OR size == 0 ----
                                new Document(
                                    "$or",
                                    Arrays.asList(
                                        new Document(
                                            "$eq",
                                            Arrays.asList(
                                                new Document("$type", "$read_locks"), "missing")),
                                        new Document(
                                            "$cond",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$size", "$read_locks"), 0)),
                                                true,
                                                false)))),

                                // ---- write_lock.holder & lease missing or null ----
                                new Document(
                                    "$and",
                                    Arrays.asList(
                                        new Document(
                                            "$or",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$type", "$write_lock.holder"),
                                                        "missing")),
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        "$write_lock.holder", BsonNull.VALUE)))),
                                        new Document(
                                            "$or",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$type", "$write_lock.lease"),
                                                        "missing")),
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        "$write_lock.lease", BsonNull.VALUE)))))))),

                        // ================= then =================
                        new Document()
                            .append("write_lock", owner)
                            .append(
                                "version",
                                new Document(
                                    "$cond",
                                    Arrays.asList(
                                        new Document(
                                            "$or",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$type", "$version"),
                                                        "missing")),
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList("$version", BsonNull.VALUE)))),
                                        1L,
                                        new Document("$add", Arrays.asList("$version", 1L))))),

                        // ================= else =================
                        "$$ROOT"))));

    printScript(pipeline);
  }
}
