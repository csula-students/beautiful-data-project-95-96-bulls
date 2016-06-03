package edu.csula.datascience.acquisition;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;


/**
 * Created by Tazzie on 5/1/2016.
 *
 */
public class InnocentCollectorApp {
    private boolean DEBUG = true;

    // MongoDB variables
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> records_coll;
    private MongoCollection<Document> execution_coll;
    private MongoCollection<Document> exonerated_coll;
    private int mongoIndex = 0;

    // file names
    private final String term_tsv = "Term_Records_1991_to_2014.tsv";
    private final String executions_csv = "Executions_1986_to_2016.csv";
    private final String exonerated_csv = "Exonorated_1989_to_2016.csv";

    // Needed for records collection
    private Map<Integer, String> records_race_map = new HashMap<>();
    private Map<Integer, String> records_crime_map = new HashMap<>();


    public static void main(String[] args)  throws URISyntaxException, IOException{
        InnocentCollectorApp tester = new InnocentCollectorApp();
        tester.driver();
    }


    public void driver() throws URISyntaxException, IOException {
        if (DEBUG)
            System.out.println("Running InnocentCollectorApp");

        // establish database connection to MongoDB
        mongoClient = new MongoClient("localhost", 27017);

        // select database
        database = mongoClient.getDatabase("ProjectInnocence");

        // select records_coll by name `Records`
        records_coll = database.getCollection("records");

        //Query the Records records_coll to see if its empty
        long myDoc = records_coll.count();

        if(myDoc == 0) {

            // read file and extract info from file
            //getData(term_tsv);
        }


        execution_coll = database.getCollection("execute");

//        execution_coll.drop();

        long myDoc2 = execution_coll.count();

        if(myDoc2 == 0) {

            // read file and extract info from file
            getData(executions_csv);
        }


        exonerated_coll = database.getCollection("exonerated");
//        exonerated_coll.drop();

        long myDoc3 = exonerated_coll.count();

        if(myDoc3 == 0) {

            // read file and extract info from file
            getData(exonerated_csv);
        }

        // mungee files
        setRecordsRaceMap();
        mungeeData(executions_csv);
        mungeeData(exonerated_csv);
        mungeeData(term_tsv);
    }

    private void getData(String filename) throws URISyntaxException, IOException {

        // get file via ClassLoader > Resource folder
        File file = new File(
                ClassLoader.getSystemResource(filename)
                        .toURI()
        );


        BufferedReader bufferFile = new BufferedReader(new FileReader(file));   // This will be our reader file
        String[] dataRowArray;                                                  // data for each row
        String dataRow = bufferFile.readLine();                                 // Read first line.
        int counter = 0;                                                        // tracks number of records

        // loop to traverse through file(s) dataRow != null
        while (dataRow != null){

            // break down data row and store into an array for easy index access
            // tab delimiter
            if (filename.contains(".tsv"))
                dataRowArray = dataRow.split("\\t");
                // comma delimiter
            else if (filename.contains(".csv"))
                dataRowArray = dataRow.split(",");
                // use a default... just in case
            else
                dataRowArray = dataRow.split("\\t");

            // exhonorated handling
            if (filename.equals(exonerated_csv))
                addExoneratedToMongo(dataRowArray, mongoIndex);

            // executions handling
            else if (filename.equals(executions_csv))
                addExecutionsToMongo(dataRowArray, mongoIndex);

            // term_records handling
            else
                addTermToMongo(dataRowArray, mongoIndex);

            dataRow = bufferFile.readLine(); // Read next line of data.
            System.out.println(counter);
            counter++;
        }

        // Close the file once all data has been read.
        bufferFile.close();

        System.out.println("Total records: " + counter);

        // End the printout with a blank line.
        System.out.println();
    }

    private void addTermToMongo(String[] rowDataArray, int index){
        // to create new document
        Document info = new Document("Inmate_ID", rowDataArray[0])
                .append("Sex", rowDataArray[1])
                .append("Admit_Type", rowDataArray[2])
                .append("Offense_General", rowDataArray[3])
                .append("Admit_Year", rowDataArray[5])
                .append("Mandatory_Release_Year", rowDataArray[7])
                .append("Projected_Release_Year", rowDataArray[8])
                .append("Parole_Eligibility_Year", rowDataArray[9])
                .append("Sentence_Length", rowDataArray[10])
                .append("Offense_Detail", rowDataArray[11])
                .append("Race", rowDataArray[12])
                .append("Age_Admitted", rowDataArray[13])
                .append("Age_Release", rowDataArray[14])
                .append("Time_Served", rowDataArray[15])
                .append("Release_Type", rowDataArray[16])
                .append("State", rowDataArray[17]);

        // filter race
        if (!rowDataArray[12].isEmpty()) {
            // filter state
            if (!rowDataArray[17].isEmpty()) {
                // filter admit year
                if (!rowDataArray[5].isEmpty()) {
                    // filter crime
                    if (!rowDataArray[3].isEmpty()) {
                        // to insert document
                        records_coll.insertOne(info);
                    }
                }
            }
        }
    }

    private void addExecutionsToMongo(String[] rowDataArray, int index){
        // to create new document
        Document info = new Document("Year", rowDataArray[0])
                .append("Age", rowDataArray[2])
                .append("Race", rowDataArray[4])
                .append("State", rowDataArray[6])
                .append("Crime", "Murder");

        // filter year
        if (!rowDataArray[0].isEmpty()){
            // filter race
            if (!rowDataArray[4].isEmpty()) {
                // filter state
                if (!rowDataArray[6].isEmpty()) {
                    // to insert document
                    execution_coll.insertOne(info);
                }
            }
        }
    }

    private void addExoneratedToMongo(String[] rowDataArray, int index){
        // to create new document
        Document info = new Document("Race", rowDataArray[3])
                .append("State", rowDataArray[5])
                .append("Worst_Crime", rowDataArray[8])
                .append("Convicted", rowDataArray[11])
                .append("Exonerated", rowDataArray[12])
                .append("Sentence", rowDataArray[13])
                .append("OM", rowDataArray[20]);

        // filter race
        if (!rowDataArray[3].isEmpty()){
            // filter state
            if (!rowDataArray[5].isEmpty()) {
                // filter Official misconduct
                if (!rowDataArray[20].isEmpty()) {
                    // to insert document
                    exonerated_coll.insertOne(info);
                }
            }
        }
    }

    /*
        swaps date field and replace with year field
        @param file_name - data resource file name
     */
    private void mungeeData(String file_name){
        System.out.println("In Mungee Data");
        // Executions collection
        if (file_name.equals(executions_csv)){
            // change Year field to Integer
            yearToInt(execution_coll, "Year");

            // change age field to integer
            stringToInt(execution_coll, "Age");
        }

        // Exonerated collection
        else if (file_name.equals(exonerated_csv)){
            System.out.println("Working on Exonerated");
            // convert sentence to integer
            sentenceToInt(exonerated_coll, "Sentence");

            // change Convicted field to integer
            stringToInt(exonerated_coll, "Convicted");

            // change Exonerated to integer
            stringToInt(exonerated_coll, "Exonerated");
        }

        // Records Collection
        else{
            normalizeRecords(records_coll, "Race");
            normalizeRecords(records_coll, "Offense_General");
        }

    }

    /*
        Changes year field to integer
        @param coll - mongo collection
        @param field - column or field of collection
     */
    private void yearToInt(MongoCollection<Document> coll, String field){
        MongoCursor<Document> cursor = coll.find().iterator();
        cursor.next();  // skip the header
        try{
            int yearNum = 0;
            Document doc;
            while (cursor.hasNext()){
                // retrieve current document
                doc = cursor.next();

                // get year from DATE field
                try{
                    yearNum = Integer.parseInt(doc.get(field).toString().split("/")[2]);
                }
                catch (ArrayIndexOutOfBoundsException e){
                    // if Year is not in DATE format, skip rest of instructions
                    return;
                }

                // Replace old value with updated value
                doc.replace(field, yearNum);

                // replace old document with updated document via _id
                coll.replaceOne(eq("_id", doc.get("_id")), doc);
            }
        }
        catch(Exception e){
            System.err.println("Error. Could not replace DATE with YEAR.");
            e.printStackTrace();
        }
        // close cursor
        cursor.close();
    }

    /*
        Converts String to Int
        @param coll - mongo collection
        @param field - column or field of collection
     */
    private void stringToInt(MongoCollection<Document> coll, String field){
        MongoCursor<Document> cursor = coll.find().iterator();
        cursor.next();  // skip the header
        int num = 0;
        Document doc;
        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();

            try{
                num = Integer.parseInt(doc.get(field).toString());

                // Replace old value with updated value
                doc.replace(field, num);

                // replace old document with updated document via _id
                coll.replaceOne(eq("_id", doc.get("_id")), doc);
            }
            catch (NumberFormatException e){
                // if field is not header and is not a number, skip rest of instructions
                System.err.println("Cannot convert to an integer.");
            }
        }
        // close cursor
        cursor.close();
    }

    /*
    Converts Sentence to Integer
    @param coll - mongo collection
    @param field - column or field of collection
 */
    private void sentenceToInt(MongoCollection<Document> coll, String field){
        MongoCursor<Document> cursor = coll.find().iterator();
        cursor.next();  // skip the header
        int year = 0;
        Document doc;
        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();

            try{
                year = Integer.parseInt(doc.get(field).toString().split(" ")[0]);

                // ignore the 2 records with YEAR as a sentence
                if (year < 2000) {
                    // Replace old value with updated value
                    doc.replace(field, year);

                    // replace old document with updated document via _id
                    coll.replaceOne(eq("_id", doc.get("_id")), doc);
                }
            }
            catch (NumberFormatException e){
                // ignore and skip record
            }
        }
        // close cursor
        cursor.close();
    }

    // Maps out race to respective integer value from data source
    private void setRecordsRaceMap(){
        records_race_map.put(1, "White");
        records_race_map.put(2, "Black");
        records_race_map.put(3, "Hispanic");
        records_race_map.put(4, "Other");
        records_race_map.put(9, "Missing");
    }

    // Maps out Offense_General (crime) to respective integer value from data source
    private void setRecordsCrimeMap(){
        records_crime_map.put(1, "Violent");
        records_crime_map.put(2, "Property");
        records_crime_map.put(3, "Drugs");
        records_crime_map.put(4, "Public Order");
        records_crime_map.put(5, "Other/Unspecified");
        records_crime_map.put(9, "Missing");
    }

    private void normalizeRecords(MongoCollection<Document> coll, String field){
        MongoCursor<Document> cursor = coll.find().iterator();
        cursor.next();  // skip the header
        Document doc;
        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();

            try{
                if (field.equals("Race"))
                    doc.replace(field, records_race_map.get(doc.get(field)));
                else
                    doc.replace(field, records_crime_map.get(doc.get(field)));
                // replace old document with updated document via _id
                coll.replaceOne(eq("_id", doc.get("_id")), doc);
            }
            catch (Exception e){
                System.err.println("Failed to normalize data");
            }
        }
        // close cursor
        cursor.close();
    }
}


