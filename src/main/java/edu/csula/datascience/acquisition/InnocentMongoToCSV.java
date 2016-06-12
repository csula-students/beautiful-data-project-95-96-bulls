package edu.csula.datascience.acquisition;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import java.io.FileWriter;


/**
 * Created by Tazzie on 6/1/2016.
 *
 */
public class InnocentMongoToCSV {
    private boolean DEBUG = true;
    private long myTime = new Date().getTime();

    // MongoDB variables
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> term_records_coll;
    private MongoCollection<Document> execution_coll;
    private MongoCollection<Document> exonerated_coll;

    // file names
    private final String term_records_fileName = "Term_Records.csv";
    private final String executions_fileName = "Executions.csv";
    private final String exonerated_fileName = "Exonerated.csv";


    public static void main(String[] args) throws URISyntaxException, IOException{
        InnocentMongoToCSV tester = new InnocentMongoToCSV();
        tester.driver();
    }


    public void driver() throws URISyntaxException, IOException {
        if (DEBUG)
            System.out.println("Running InnocentMongoToCSV");

        // establish database connection to MongoDB
        mongoClient = new MongoClient("localhost", 27017);

        // select database
        database = mongoClient.getDatabase("project_innocence");

        // Assign collections
        term_records_coll = database.getCollection("records");
        execution_coll = database.getCollection("execute");
        exonerated_coll = database.getCollection("exonerated");

        outputData(execution_coll, executions_fileName);
        outputData(exonerated_coll, exonerated_fileName);
        outputData(term_records_coll, term_records_fileName);

        myTime = new Date().getTime() - myTime;
        System.out.println("Program ran for\n" + myTime + " ms"
                + "\nor " + myTime/1000 + " seconds"
                + "\nor " + myTime/(1000*60) + " minutes"
                + "\nor " + myTime/(1000*60*60) + " hours");
    }

    private void outputData(MongoCollection<Document> coll, String filename) throws URISyntaxException, IOException {
        MongoCursor<Document> cursor = coll.find().iterator();
        Document doc;
        String doc_value;
        int counter = 0;
        boolean firstItem = true;
        boolean firstLine = true;

        FileWriter writer = new FileWriter(filename);
        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();
            firstItem = true;
            System.out.println("RecordNum: " + counter++);

            for (String s: doc.keySet()){
                // ignore _id
//                if (s.equals("_id"))
//                    continue;
                // wrap quotes around because some entries have commas
                doc_value = cleanString(cleanString(doc.get(s).toString(), ","), "\"");
                if (firstItem && firstLine){
                    writer.append("Id");
                    firstItem = false;
                    firstLine = false;
                }
                else if (firstItem){
                    writer.append(doc_value);
                    firstItem = false;
                }
                else{
                    writer.append(',');
                    writer.append(doc_value);
                }
            }
            writer.append('\n');
        }
        writer.close();
    }

    private String cleanString(String inString, String regex){
        if (!inString.contains(regex))
            return inString;

        String result = "";
        boolean firstItem = true;
        String[] parseIn = inString.split(regex);
        if (regex.equals(",")) {
            for (String s : parseIn) {
                if (firstItem)
                    result += s;
                else {
                    result += "/";
                    result += s;
                }
            }
        }
        else{
            for (String s : parseIn)
                result += s;
        }

        System.out.println("result: " + result);
        return result;
    }
}
