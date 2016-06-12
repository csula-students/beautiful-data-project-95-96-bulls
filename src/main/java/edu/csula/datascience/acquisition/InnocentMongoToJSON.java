package edu.csula.datascience.acquisition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;



public class InnocentMongoToJSON {

    // MongoDB variables
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> term_records_coll;
    private MongoCollection<Document> execution_coll;
    private MongoCollection<Document> exonerated_coll;

    // file names
    private final String term_records_fileName = "Term_Records.json";
    private final String executions_fileName = "Executions.json";
    private final String exonerated_fileName = "Exonerated.json";

    public static void main(String[] args) {
        InnocentMongoToJSON tester = new InnocentMongoToJSON();
        tester.driver();
    }

    public void driver(){
        // establish database connection to MongoDB
        mongoClient = new MongoClient("localhost", 27017);

        // select database
        database = mongoClient.getDatabase("project_innocence");

        // Assign collections
        term_records_coll = database.getCollection("records");
        execution_coll = database.getCollection("execute");
        exonerated_coll = database.getCollection("exonerated");

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(executions_fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }


        for (Document doc : execution_coll.find()) {
            try {
                writer.write(doc.toJson());
                writer.newLine();

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        mongoClient.close();

        System.out.println("Ding!!!");

    }
}
