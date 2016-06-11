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
import java.sql.Time;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;


/**
 * Created by Tazzie on 5/1/2016.
 *
 */
public class InnocentCollectorApp {
    private boolean DEBUG = true;
    private long myTime = new Date().getTime();
    private final int maxRecods = 12000000;

    // MongoDB variables
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> records_coll;
    private MongoCollection<Document> execution_coll;
    private MongoCollection<Document> exonerated_coll;
    private int mongoIndex = 0;

    // file names
    private final String term_records_fileName = "Term_Records_1991_to_2014.tsv";
    private final String executions_fileName = "Executions_1986_to_2016.csv";
    private final String exonerated_fileName = "Exonorated_1989_to_2016.csv";

    // normalized data
    private Map<String, String> state_map = new HashMap<>();
    private Map<String, String> region_map = new HashMap<>();
    private Map<String, String> race_map = new HashMap<>();

    // Needed for records collection
    private Map<String, String> records_sex_map = new HashMap<>();
    private Map<String, String> records_admit_type_map = new HashMap<>();
    private Map<String, String> records_offense_general_map = new HashMap<>();
    private Map<String, String> records_sentence_length_map = new HashMap<>();
    private Map<String, String> records_offense_detail_map = new HashMap<>();
    private Map<String, String> records_age_admit_map = new HashMap<>();
    private Map<String, String> records_age_release_map = new HashMap<>();
    private Map<String, String> records_time_served_map = new HashMap<>();
    private Map<String, String> records_release_type_map = new HashMap<>();

    private List<String> execution_races_list = new ArrayList<>();
    private List<String> exonerated_races_list = new ArrayList<>();
    private List<String> exonerated_om_list = new ArrayList<>();

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
        database = mongoClient.getDatabase("project_innocence");

        // select records_coll by name `Records`
        records_coll = database.getCollection("records");

        //Query the Records records_coll to see if its empty
        long myDoc = records_coll.count();

        if(myDoc == 0) {

            // read file and extract info from file
            getData(term_records_fileName);
        }


        execution_coll = database.getCollection("execute");

//        execution_coll.drop();

        long myDoc2 = execution_coll.count();

        if(myDoc2 == 0) {

            // read file and extract info from file
            getData(executions_fileName);
        }


        exonerated_coll = database.getCollection("exonerated");
//        exonerated_coll.drop();

        long myDoc3 = exonerated_coll.count();

        if(myDoc3 == 0) {

            // read file and extract info from file
            getData(exonerated_fileName);
        }

        // mungee files
        setStateMap();
        setRegionMap();
        setRecordsRaceMap();
        setRecordsOffenseGeneralMap();
        setRecordsSexMap();
        setRecordsAdmitTypeMap();
        setRecordsSentenceLengthMap();
        setRecordsOffenseDetailMap();
        setRecordsAgeAdmitMap();
        setRecordsAgeReleaseMap();
        setRecordsTimeServedMap();
        setReleaseTypeMap();

        mungeeData(executions_fileName);
        mungeeData(exonerated_fileName);
        mungeeData(term_records_fileName);

        System.out.println("Races in execution records");
        for (String s: execution_races_list){
            System.out.println(s);
        }

        System.out.println("Races in exonerated records");
        for (String s: exonerated_races_list){
            System.out.println(s);
        }

        System.out.println("OM in exonerated records");
        for (String s: exonerated_om_list){
            System.out.println(s);
        }
        myTime = new Date().getTime() - myTime;
        System.out.println("Program ran for\n" + myTime + " ms"
            + "\nor " + myTime/1000 + " seconds"
            + "\nor " + myTime/(1000*60) + " minutes"
            + "\nor " + myTime/(1000*60*60) + " hours");
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
        while (dataRow != null && counter < maxRecods){

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
            if (filename.equals(exonerated_fileName))
                addExoneratedToMongo(dataRowArray, mongoIndex);

            // executions handling
            else if (filename.equals(executions_fileName))
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

        if (!execution_races_list.contains(rowDataArray[4]))
            execution_races_list.add(rowDataArray[4]);
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
        if (!exonerated_races_list.contains(rowDataArray[3]))
            exonerated_races_list.add(rowDataArray[3]);

        if (!exonerated_om_list.contains(rowDataArray[20]))
            exonerated_om_list.add(rowDataArray[20]);
    }

    private void addRegionToMongo(MongoCollection<Document> coll){
        MongoCursor<Document> cursor = coll.find().iterator();
        cursor.next();  // skip the header
        String state;
        Document doc;
        String fileName;
        if (coll.equals(execution_coll))
            fileName = executions_fileName;
        else if (coll.equals(exonerated_coll))
            fileName = exonerated_fileName;
        else
            fileName = term_records_fileName;
        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();
            try{
                state = doc.get("State").toString();
                if (!state.equals(null) && !state.equals("") && !state.equals(" "))
                    doc.append("Region", region_map.get(state));
                else {
                    System.err.println("Error. Could not add region. Filename: " + fileName
                            + ". ID: " + doc.get("_id")
                            + ". State: " + state);
                }
            }
            catch(Exception e){
                System.err.println("Error. Could not add region. Filename: " + fileName
                        + ". ID: " + doc.get("_id"));
                e.printStackTrace();
            }
            // replace old document with updated document via _id
            coll.replaceOne(eq("_id", doc.get("_id")), doc);
        }
        // close cursor
        cursor.close();
    }

    /*
        swaps date field and replace with year field
        @param file_name - data resource file name
     */
    private void mungeeData(String file_name){
        System.out.println("In Mungee Data, " + file_name);
        // Executions collection
        if (file_name.equals(executions_fileName)){

            // remove data
            removeData(execution_coll, "State");
            removeData(execution_coll, "Race");

            // change Year field to Integer
            yearToInt(execution_coll, "Year");

            // change age field to integer
            stringToInt(execution_coll, "Age");

            // normalize data
            normalizeData(execution_coll, "Race");
            normalizeData(execution_coll, "State");

            // add region to collection
            addRegionToMongo(execution_coll);
        }

        // Exonerated collection
        else if (file_name.equals(exonerated_fileName)){
            System.out.println("Working on Exonerated");

            // remove data
            removeData(exonerated_coll, "State");

            // convert sentence to integer
            sentenceToInt(exonerated_coll, "Sentence");

            // change Convicted field to integer
            stringToInt(exonerated_coll, "Convicted");

            // change Exonerated to integer
            stringToInt(exonerated_coll, "Exonerated");

            // normalize data
            normalizeData(exonerated_coll, "Race");

            // add region to collection
            addRegionToMongo(exonerated_coll);
        }

        // Records Collection
        else{
            // remove data
            removeData(records_coll, "Race");

            normalizeTermRecordsData();

//            normalizeData(records_coll, "Race");
//            normalizeData(records_coll, "Offense_General");
//            normalizeData(records_coll, "State");
//            normalizeData(records_coll, "Sex");
//            normalizeData(records_coll, "Admit_Type");
//            normalizeData(records_coll, "Sentence_Length");
//            normalizeData(records_coll, "Offense_Detail");
//            normalizeData(records_coll, "Age_Admitted");
//            normalizeData(records_coll, "Age_Release");
//            normalizeData(records_coll, "Time_Served");

            // add region to collection
            //addRegionToMongo(records_coll);
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
                System.err.println("Cannot convert to an integer. Field " + field + ".");
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
        race_map.put("1", "White");
        race_map.put("2", "Black");
        race_map.put("3", "Hispanic");
        race_map.put("4", "Other");
//        race_map.put("9", "Missing");

        race_map.put("Caucasian", "White");
        race_map.put("White", "White");
        race_map.put("Black", "Black");

        race_map.put("Latino", "Hispanic");
        race_map.put("Hispanic", "Hispanic");

        race_map.put("Asian", "Other");
        race_map.put("Native American", "Other");
        race_map.put("Other", "Other");
    }

    // Maps out Offense_General (crime) to respective integer value from data source
    private void setRecordsOffenseGeneralMap(){
        records_offense_general_map.put("1", "Violent");
        records_offense_general_map.put("2", "Property");
        records_offense_general_map.put("3", "Drugs");
        records_offense_general_map.put("4", "Public Order");
        records_offense_general_map.put("5", "Other/Unspecified");
        records_offense_general_map.put("9", "Missing");
        records_offense_general_map.put("", "Missing");
        records_offense_general_map.put(" ", "Missing");
    }

    // Maps out Offense_General (crime) to respective integer value from data source
    private void setStateMap(){
        state_map.put("1", "Alabama");
        state_map.put("2", "Alaska");
        state_map.put("4", "Arizona");
        state_map.put("5", "Arkansas");
        state_map.put("6", "California");
        state_map.put("8", "Colorado");
        state_map.put("9", "Connecticut");
        state_map.put("10", "Delaware");
        state_map.put("11", "District of Columbia");
        state_map.put("12", "Florida");
        state_map.put("13", "Georgia");
        state_map.put("15", "Hawaii");
        state_map.put("16", "Idaho");
        state_map.put("17", "Illinois");
        state_map.put("18", "Indiana");
        state_map.put("19", "Iowa");
        state_map.put("20", "Kansas");
        state_map.put("21", "Kentucky");
        state_map.put("22", "Louisiana");
        state_map.put("23", "Maine");
        state_map.put("24", "Maryland");
        state_map.put("25", "Massachusetts");
        state_map.put("26", "Michigan");
        state_map.put("27", "Minnesota");
        state_map.put("28", "Mississippi");
        state_map.put("29", "Missouri");
        state_map.put("30", "Montana");
        state_map.put("31", "Nebraska");
        state_map.put("32", "Nevada");
        state_map.put("33", "New Hampshire");
        state_map.put("34", "New Jersey");
        state_map.put("35", "New Mexico");
        state_map.put("36", "New York");
        state_map.put("37", "North Carolina");
        state_map.put("38", "North Dakota");
        state_map.put("39", "Ohio");
        state_map.put("40", "Oklahoma");
        state_map.put("41", "Oregon");
        state_map.put("42", "Pennsylvania");
        state_map.put("44", "Rhode Island");
        state_map.put("45", "South Carolina");
        state_map.put("46", "South Dakota");
        state_map.put("47", "Tennessee");
        state_map.put("48", "Texas");
        state_map.put("49", "Utah");
        state_map.put("50", "Vermont");
        state_map.put("51", "Virginia");
        state_map.put("53", "Washington");
        state_map.put("54", "West Virginia");
        state_map.put("55", "Wisconsin");
        state_map.put("56", "Wyoming");

        state_map.put("AL", "Alabama");
        state_map.put("AK", "Alaska");
        state_map.put("AZ", "Arizona");
        state_map.put("AR", "Arkansas");
        state_map.put("CA", "California");
        state_map.put("CO", "Colorado");
        state_map.put("CT", "Connecticut");
        state_map.put("DE", "Delaware");
        state_map.put("DC", "District of Columbia");
        state_map.put("FL", "Florida");
        state_map.put("GA", "Georgia");
        state_map.put("HI", "Hawaii");
        state_map.put("ID", "Idaho");
        state_map.put("IL", "Illinois");
        state_map.put("IN", "Indiana");
        state_map.put("IA", "Iowa");
        state_map.put("KS", "Kansas");
        state_map.put("KY", "Kentucky");
        state_map.put("LA", "Louisiana");
        state_map.put("ME", "Maine");
        state_map.put("MD", "Maryland");
        state_map.put("MA", "Massachusetts");
        state_map.put("MI", "Michigan");
        state_map.put("MN", "Minnesota");
        state_map.put("MS", "Mississippi");
        state_map.put("MO", "Missouri");
        state_map.put("MT", "Montana");
        state_map.put("NE", "Nebraska");
        state_map.put("NV", "Nevada");
        state_map.put("NH", "New Hampshire");
        state_map.put("NJ", "New Jersey");
        state_map.put("NM", "New Mexico");
        state_map.put("NY", "New York");
        state_map.put("NC", "North Carolina");
        state_map.put("ND", "North Dakota");
        state_map.put("OH", "Ohio");
        state_map.put("OK", "Oklahoma");
        state_map.put("OR", "Oregon");
        state_map.put("PA", "Pennsylvania");
        state_map.put("RI", "Rhode Island");
        state_map.put("SC", "South Carolina");
        state_map.put("SD", "South Dakota");
        state_map.put("TN", "Tennessee");
        state_map.put("TX", "Texas");
        state_map.put("UT", "Utah");
        state_map.put("VT", "Vermont");
        state_map.put("VA", "Virginia");
        state_map.put("WA", "Washington");
        state_map.put("WV", "West Virginia");
        state_map.put("WI", "Wisconsin");
        state_map.put("WY", "Wyoming");

        state_map.put("Alabama", "Alabama");
        state_map.put("Alaska", "Alaska");
        state_map.put("Arizona", "Arizona");
        state_map.put("Arkansas", "Arkansas");
        state_map.put("California", "California");
        state_map.put("Colorado", "Colorado");
        state_map.put("Connecticut", "Connecticut");
        state_map.put("Delaware", "Delaware");
        state_map.put("District of Columbia", "District of Columbia");
        state_map.put("Florida", "Florida");
        state_map.put("Georgia", "Georgia");
        state_map.put("Hawaii", "Hawaii");
        state_map.put("Idaho", "Idaho");
        state_map.put("Illinois", "Illinois");
        state_map.put("Indiana", "Indiana");
        state_map.put("Iowa", "Iowa");
        state_map.put("Kansas", "Kansas");
        state_map.put("Kentucky", "Kentucky");
        state_map.put("Louisiana", "Louisiana");
        state_map.put("Maine", "Maine");
        state_map.put("Maryland", "Maryland");
        state_map.put("Massachusetts", "Massachusetts");
        state_map.put("Michigan", "Michigan");
        state_map.put("Minnesota", "Minnesota");
        state_map.put("Mississippi", "Mississippi");
        state_map.put("Missouri", "Missouri");
        state_map.put("Montana", "Montana");
        state_map.put("Nebraska", "Nebraska");
        state_map.put("Nevada", "Nevada");
        state_map.put("New Hampshire", "New Hampshire");
        state_map.put("New Jersey", "New Jersey");
        state_map.put("New Mexico", "New Mexico");
        state_map.put("New York", "New York");
        state_map.put("North Carolina", "North Carolina");
        state_map.put("North Dakota", "North Dakota");
        state_map.put("Ohio", "Ohio");
        state_map.put("Oklahoma", "Oklahoma");
        state_map.put("Oregon", "Oregon");
        state_map.put("Pennsylvania", "Pennsylvania");
        state_map.put("Rhode Island", "Rhode Island");
        state_map.put("South Carolina", "South Carolina");
        state_map.put("South Dakota", "South Dakota");
        state_map.put("Tennessee", "Tennessee");
        state_map.put("Texas", "Texas");
        state_map.put("Utah", "Utah");
        state_map.put("Vermont", "Vermont");
        state_map.put("Virginia", "Virginia");
        state_map.put("Washington", "Washington");
        state_map.put("West Virginia", "West Virginia");
        state_map.put("Wisconsin", "Wisconsin");
        state_map.put("Wyoming", "Wyoming");
    }

    private void setRegionMap(){
        region_map.put("Alabama", "South");
        region_map.put("Alaska", "West");
        region_map.put("Arizona", "West");
        region_map.put("Arkansas", "South");
        region_map.put("California", "West");
        region_map.put("Colorado", "West");
        region_map.put("Connecticut", "Northeast");
        region_map.put("Delaware", "South");
        region_map.put("District of Columbia", "South");
        region_map.put("Florida", "South");
        region_map.put("Georgia", "South");
        region_map.put("Hawaii", "West");
        region_map.put("Idaho", "West");
        region_map.put("Illinois", "Midwest");
        region_map.put("Indiana", "Midwest");
        region_map.put("Iowa", "Midwest");
        region_map.put("Kansas", "Midwest");
        region_map.put("Kentucky", "South");
        region_map.put("Louisiana", "South");
        region_map.put("Maine", "Northeast");
        region_map.put("Maryland", "South");
        region_map.put("Massachusetts", "Northeast");
        region_map.put("Michigan", "Midwest");
        region_map.put("Minnesota", "Midwest");
        region_map.put("Mississippi", "South");
        region_map.put("Missouri", "Midwest");
        region_map.put("Montana", "West");
        region_map.put("Nebraska", "Midwest");
        region_map.put("Nevada", "West");
        region_map.put("New Hampshire", "Northeast");
        region_map.put("New Jersey", "Northeast");
        region_map.put("New Mexico", "West");
        region_map.put("New York", "Northeast");
        region_map.put("North Carolina", "South");
        region_map.put("North Dakota", "Midwest");
        region_map.put("Ohio", "Midwest");
        region_map.put("Oklahoma", "South");
        region_map.put("Oregon", "West");
        region_map.put("Pennsylvania", "Northeast");
        region_map.put("Rhode Island", "Northeast");
        region_map.put("South Carolina", "South");
        region_map.put("South Dakota", "Midwest");
        region_map.put("Tennessee", "South");
        region_map.put("Texas", "South");
        region_map.put("Utah", "West");
        region_map.put("Vermont", "Northeast");
        region_map.put("Virginia", "South");
        region_map.put("Washington", "West");
        region_map.put("West Virginia", "South");
        region_map.put("Wisconsin", "Midwest");
        region_map.put("Wyoming", "West");
    }

    private void setRecordsTimeServedMap(){
        records_time_served_map.put("0", "< 1 year");
        records_time_served_map.put("1", "1-1.9 years");
        records_time_served_map.put("2", "2-4.9 years");
        records_time_served_map.put("3", "5-9.9 years");
        records_time_served_map.put("4", ">= 10 years");
        records_time_served_map.put("9", "Missing");
        records_time_served_map.put("", "Missing");
        records_time_served_map.put(" ", "Missing");
    }

    private void setRecordsSexMap(){
        records_sex_map.put("1", "Male");
        records_sex_map.put("2", "Female");
    }

    private void setRecordsAdmitTypeMap(){
        records_admit_type_map.put("1", "New court commitment");
        records_admit_type_map.put("2", "Parole return/revocation");
        records_admit_type_map.put("3", "Other admission");
        records_admit_type_map.put("9", "Missing");
        records_admit_type_map.put("", "Missing");
        records_admit_type_map.put(" ", "Missing");
    }

    private void setRecordsSentenceLengthMap(){
        records_sentence_length_map.put("0", "< 1 year");
        records_sentence_length_map.put("1", "1-1.9 years");
        records_sentence_length_map.put("2", "2-4.9 years");
        records_sentence_length_map.put("3", "5-9.9 years");
        records_sentence_length_map.put("4", "10-24.9 years");
        records_sentence_length_map.put("5", ">=25 years");
        records_sentence_length_map.put("6", "Life, LWOP, Life plus additional years, Death");
        records_sentence_length_map.put("9", "Missing");
        records_sentence_length_map.put("", "Missing");
        records_sentence_length_map.put(" ", "Missing");
    }

    private void setRecordsOffenseDetailMap(){
        records_offense_detail_map.put("1", "Murder (including non-negligent manslaughter)");
        records_offense_detail_map.put("2", "Negligent manslaughter");
        records_offense_detail_map.put("3", "Rape/sexual assault");
        records_offense_detail_map.put("4", "Robbery");
        records_offense_detail_map.put("5", "Aggravated or simple assault");
        records_offense_detail_map.put("6", "Other violent offenses");
        records_offense_detail_map.put("7", "Burglary");
        records_offense_detail_map.put("8", "Larceny");
        records_offense_detail_map.put("9", "Motor vehicle theft");
        records_offense_detail_map.put("10", "Fraud");
        records_offense_detail_map.put("11", "Other property offenses");
        records_offense_detail_map.put("12", "Drugs (includes possession, distribution, trafficking, other)");
        records_offense_detail_map.put("13", "Public order");
        records_offense_detail_map.put("14", "Other/unspecified");
        records_offense_detail_map.put("99", "Missing");
        records_offense_detail_map.put("", "Missing");
        records_offense_detail_map.put(" ", "Missing");
    }

    private void setRecordsAgeAdmitMap(){
        records_age_admit_map.put("1", "18-24 years");
        records_age_admit_map.put("2", "25-34 years");
        records_age_admit_map.put("3", "35-44 years");
        records_age_admit_map.put("4", "45-54 years");
        records_age_admit_map.put("5", "55+ years");
        records_age_admit_map.put("9", "Missing");
        records_age_admit_map.put("", "Missing");
        records_age_admit_map.put(" ", "Missing");
    }

    private void setRecordsAgeReleaseMap(){
        records_age_release_map.put("1", "18-24 years");
        records_age_release_map.put("2", "25-34 years");
        records_age_release_map.put("3", "35-44 years");
        records_age_release_map.put("4", "45-54 years");
        records_age_release_map.put("5", "55+ years");
        records_age_release_map.put("9", "Missing");
        records_age_release_map.put("", "Missing");
        records_age_release_map.put(" ", "Missing");
    }

    private void setReleaseTypeMap(){
        records_release_type_map.put("1", "Conditional release");
        records_age_release_map.put("2", "Unconditional release");
        records_age_release_map.put("3", "Other release (including death, transfer, AWOL, escape)");
        records_age_release_map.put("9", "Missing");
        records_age_release_map.put("", "Missing");
        records_age_release_map.put(" ", "Missing");
    }

    private void removeData(MongoCollection<Document> coll, String field){
        MongoCursor<Document> cursor = coll.find().iterator();
        cursor.next();  // skip the header
        Document doc;
        String doc_value;
        String coll_name = "";

        if (coll.equals(execution_coll))
            coll_name = executions_fileName;
        else if (coll.equals(exonerated_coll))
            coll_name = exonerated_fileName;
        else
            coll_name = term_records_fileName;

        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();
            doc_value = doc.get(field).toString();

            switch(field){
                case "State":
                    if (state_map.get(doc_value) == null){
//                        System.out.println("Removing " + coll_name
//                                + " Field: " + field + " FieldValue: " + doc_value);
                        coll.deleteOne(eq("_id", doc.get("_id")));
                    }
                    break;
                default:
                    if (race_map.get(doc_value) == null || race_map.get(doc_value).equals("Missing")){
//                        System.out.println("Removing " + coll_name
//                                + " Field: " + field + " FieldValue: " + doc_value);
                        coll.deleteOne(eq("_id", doc.get("_id")));
                    }
            }
        }
        // close cursor
        cursor.close();

    }

    private void normalizeData(MongoCollection<Document> coll, String field){
        MongoCursor<Document> cursor = coll.find().iterator();
        cursor.next();  // skip the header
        Document doc;
        String doc_value;
        String temp;
        int number = -1;
        boolean isNumber;
        String coll_name = "";

        if (coll.equals(execution_coll))
            coll_name = executions_fileName;
        else if (coll.equals(exonerated_coll))
            coll_name = exonerated_fileName;
        else
            coll_name = term_records_fileName;

        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();
            isNumber = false;
            doc_value = doc.get(field).toString();

            // test if doc_value is a number instead of a text
            try {
                number = Integer.parseInt(doc_value);
                isNumber = true;
            }catch(NumberFormatException e){
                // do nothing, isNumber remains false
            }

            try{
                switch(field){
                    case "State":
//                        System.out.println("Cleaning state, field: " + field);
//                        System.out.println("Was: " + number + ", Will be: " + state_map.get(number));
                        temp = state_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    case "Offense_General":
//                        System.out.println("Cleaning crime, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_offense_general_map.get(number));
                        temp = records_offense_general_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    case "Sex":
//                        System.out.println("Cleaning sex, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_sex_map.get(number));
                        temp = records_sex_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    case "Admit_Type":
//                        System.out.println("Cleaning admit type, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_admit_type_map.get(number));
                        temp = records_admit_type_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    case "Sentence_Length":
//                        System.out.println("Cleaning sentence length, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_sentence_length_map.get(number));
                        temp = records_sentence_length_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    case "Offense_Detail":
//                        System.out.println("Cleaning Offense Detail, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_offense_detail_map.get(number));
                        doc.replace(field, records_offense_detail_map.get(doc_value));
                        break;
                    case "Age_Admitted":
//                        System.out.println("Cleaning Age_Admitted, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_age_admit_map.get(number));
                        temp = records_age_admit_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    case "Age_Release":
//                        System.out.println("Cleaning Age_Release, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_age_release_map.get(number));
                        temp = records_age_release_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    case "Time_Served":
//                        System.out.println("Cleaning Time_Served, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + records_time_served_map.get(number));
                        temp = records_time_served_map.get(doc_value);
                        if (temp.isEmpty() || temp == null)
                            doc.replace(field, "Unknown");
                        else
                            doc.replace(field, temp);
                        break;
                    default:
//                        System.out.println("Replacing race, field:" + field);
//                        System.out.println("Was: " + number + ", Will be: " + race_map.get(number));
                        if (isNumber && coll_name.equals(exonerated_fileName)) {
                            doc.replace(field, "Other");
                        }
                        else{
                            temp = race_map.get(doc_value);
                            if (temp.isEmpty() || temp == null)
                                doc.replace(field, "Unknown");
                            else
                                doc.replace(field, temp);
                        }
                }
                // replace old document with updated document via _id
                coll.replaceOne(eq("_id", doc.get("_id")), doc);
            }
            catch (Exception e){
//                System.err.println("Failed to normalize " + coll_name + ". ObjectID: " + doc.get("_id")
//                        + ". Field: " + field + ". Value: " + doc.get(field));
                coll.deleteOne(eq("_id", doc.get("_id")));
            }
        }
        // close cursor
        cursor.close();
    }

    private void normalizeTermRecordsData(){
        MongoCursor<Document> cursor = records_coll.find().iterator();
        cursor.next();  // skip the header
        Document doc;
        String field;
        String state;

        while (cursor.hasNext()){
            // retrieve current document
            doc = cursor.next();

            try{

                field = "State";
                state = state_map.get(doc.get(field).toString());

                doc = normalizeDataHelper(doc, state, field);
                try{
                    if (!state.equals(null) && !state.equals("") && !state.equals(" ")) {
                        doc.append("Region", region_map.get(state));
                    }
                    else {
                        System.err.println("Error. Could not add region. Filename: " + term_records_fileName
                                + ". ID: " + doc.get("_id")
                                + ". State: " + state);
                    }
                }
                catch(Exception e){
                    System.err.println("Error. Could not add region. Filename: " + term_records_fileName
                            + ". ID: " + doc.get("_id")
                            + ". State: " + state);
                    e.printStackTrace();
                }


                field = "Offense_General";
                doc = normalizeDataHelper(doc, records_offense_general_map.get(doc.get(field).toString()), field);

                field = "Sex";
                doc = normalizeDataHelper(doc, records_sex_map.get(doc.get(field).toString()), field);

                field = "Admit_Type";
                doc = normalizeDataHelper(doc, records_admit_type_map.get(doc.get(field).toString()), field);

                field = "Sentence_Length";
                doc = normalizeDataHelper(doc, records_sentence_length_map.get(doc.get(field).toString()), field);

                field = "Offense_Detail";
                doc = normalizeDataHelper(doc, records_offense_detail_map.get(doc.get(field).toString()), field);

                field = "Age_Admitted";
                doc = normalizeDataHelper(doc, records_age_admit_map.get(doc.get(field).toString()), field);

                field = "Age_Release";
                doc = normalizeDataHelper(doc, records_age_release_map.get(doc.get(field).toString()), field);

                field = "Time_Served";
                doc = normalizeDataHelper(doc, records_time_served_map.get(doc.get(field).toString()), field);

                field = "Race";
                doc = normalizeDataHelper(doc, race_map.get(doc.get(field).toString()), field);

                field = "Release_Type";
                doc = normalizeDataHelper(doc, records_release_type_map.get(doc.get(field).toString()), field);

                // replace old document with updated document via _id
                records_coll.replaceOne(eq("_id", doc.get("_id")), doc);
            }
            catch (Exception e){
//                System.err.println("Failed to normalize " + coll_name + ". ObjectID: " + doc.get("_id")
//                        + ". Field: " + field + ". Value: " + doc.get(field));
                records_coll.deleteOne(eq("_id", doc.get("_id")));
            }
        }
        // close cursor
        cursor.close();
    }

    private Document normalizeDataHelper(Document doc, String doc_value, String field){
        if (doc_value.isEmpty() || doc_value == null)
            doc.replace(field, "Unknown");
        else
            doc.replace(field, doc_value);
        return doc;
    }
}
