package edu.csula.datascience.acquisition;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by Tazzie on 5/1/2016.
 */
public class InnocentCollectorApp {
    private boolean DEBUG = true;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public static void main(String[] args)  throws URISyntaxException, IOException{
        InnocentCollectorApp tester = new InnocentCollectorApp();
        tester.driver();
    }


    public void driver() throws URISyntaxException, IOException {
        if (DEBUG)
            System.out.println("Running InnocentCollectorApp");

        // establish database connection to MongoDB
        mongoClient = new MongoClient("db", 27017);
        // select database
        database = mongoClient.getDatabase("ProjectInnocence");

        // select collection by name `test`
        collection = database.getCollection("test");

        // read file and extract info from file
        getData("Term_Records_1991_to_2014.tsv");

    }

    private void getData(String filename) throws URISyntaxException, IOException {

        File tsv = new File(
                ClassLoader.getSystemResource(filename)
                        .toURI()
        );

        //StringTokenizer st;
        BufferedReader TSVFile = new BufferedReader(new FileReader(tsv));
        List<String> allDataArray = new ArrayList<>();
        String[] dataRowArray;
        String dataRow = TSVFile.readLine(); // Read first line.
        //dataRow = TSVFile.readLine(); // Go to next line.
        int counter = 0;
        int index = 0;
        int mongoIndex = 0;

        // total records = 10907334
        // counter sentinel must be <= 9000000 to store in arraylist without being out of memory
        while (dataRow != null && counter < 10){
            //System.out.println("Counter: " + counter);
            //st = new StringTokenizer(dataRow,"\\t");
            //System.out.println("dataRow contains: " + dataRow);
            dataRowArray = dataRow.split("\\t");
            index = 0;

            // Is education or race not recorded? If so, skip record
            if (dataRowArray[4].isEmpty() || dataRowArray[12].isEmpty()) {
                System.out.println("Record skipped: " + counter);
            }
            else {
                for (String s : dataRowArray) {
                    System.out.println("s: " + s + "\ti: " + index);
                    index++;
                }
                addToMongo(dataRowArray, mongoIndex);
                mongoIndex++;
            }

            dataRow = TSVFile.readLine(); // Read next line of data.

            counter++;
        }
        // Close the file once all data has been read.
        TSVFile.close();

        if (DEBUG) {
            for (String item : allDataArray) {
                //System.out.println(item); // Print the data line.
            }
            System.out.println("done with dataArray");
        }

        System.out.println("Total records: " + counter);

        // End the printout with a blank line.
        System.out.println();
    }

    private void addToMongo(String[] rowDataArray, int index){
        // to create new document
        Document info = new Document("ABT_INMATE_ID", rowDataArray[0])
                .append("SEX", rowDataArray[1])
                .append("ADMTYPE", rowDataArray[2])
                .append("OFFGENERAL", rowDataArray[3])
                .append("EDUCATION", rowDataArray[4])
                .append("ADMITYR", rowDataArray[5])
                .append("RELEASEYR", rowDataArray[6])
                .append("MAND_PRISREL_YEAR", rowDataArray[7])
                .append("PROJ_PRISREL_YEAR", rowDataArray[8])
                .append("PARELIG_YEAR", rowDataArray[9])
                .append("SENTLGTH", rowDataArray[10])
                .append("OFFDETAIL", rowDataArray[11])
                .append("RACE", rowDataArray[12])
                .append("AGEADMIT", rowDataArray[13])
                .append("AGERELEASE", rowDataArray[14])
                .append("TIMESRVD", rowDataArray[15])
                .append("RELTYPE", rowDataArray[16])
                .append("STATE", rowDataArray[17]);

        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("record_id", index)
                .append("info", info);

        // to insert document
        collection.insertOne(doc);
        System.out.println(
                String.format(
                        "Inserted new document %s",
                        doc.toJson()
                )
        );

    }

}
