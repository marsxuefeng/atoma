package atoma.test.mutex;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IncTest {

    public static void main(String[] args) throws InterruptedException {
        MongoClient mongoClient = MongoClients.create(
                "mongodb://127.0.0.1:32768/atoma_test"
        );

        MongoDatabase db = mongoClient.getDatabase("atoma_test");

        MongoCollection<Document> collection = db.getCollection("inc_test");
        //collection.insertOne(new Document("_id","1").append("value", 0L));

        ExecutorService executorService = Executors.newFixedThreadPool(48);
        for(int i = 0; i < 48; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    db.getCollection("inc_test").updateOne(
                            Filters.eq("_id","1"),
                            Updates.inc("value",1)
                    );
                }
            });
        }

        TimeUnit.SECONDS.sleep(30);
    }

}


