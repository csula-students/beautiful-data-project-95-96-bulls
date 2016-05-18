package edu.csula.datascience.acquisition;

import com.mongodb.DB;
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
        mongoClient = new MongoClient("localhost", 27017);

        // select database
        database = mongoClient.getDatabase("ProjectInnocence");

        // select collection by name `test`
        collection = database.getCollection("test");

        collection.insertOne(new Document("address", new Document ()
                .append("street", "123 4th Street")
                .append("city", "Los Angeles")
                .append("state", "CA")
                .append("zipcode", "91111")));

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
        List<Integer> records_col_list = new ArrayList<>();
        setRecordColList(records_col_list);

        // total records = 10907334
        // counter sentinel must be <= 9000000 to store in arraylist without being out of memory
        while (dataRow != null && counter < 10){
            //System.out.println("Counter: " + counter);
            //st = new StringTokenizer(dataRow,"\\t");
            //System.out.println("dataRow contains: " + dataRow);
            dataRowArray = dataRow.split("\\t");
            index = 0;

            /*
            1. abt_inmate_id	--> how to account for duplicates?
             2. ageadmit
             3. education
             4. race
             5. state
             6. offgeneral
             7. offdetail
             8. admityr
             9. sentlgth
            10. timesrvd
            11. reltype
             */

            // Is education or race not recorded? If so, skip record
            if (dataRowArray[4].isEmpty() || dataRowArray[12].isEmpty()) {
                System.out.println("Record skipped: " + counter);
            }
            else {
                for (String s : dataRowArray) {
                    if (records_col_list.contains(index))
                        handleRecord(s, index);
                    index++;
                }
                //addToMongo(dataRowArray, mongoIndex);
                //mongoIndex++;
            }

            // common factors of data: race, state, crime, approximate time (year)

            //

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

    private void handleRecord(String record, int index){
        System.out.println("s: " + record + "\ti: " + index);
    }

    private void setRecordColList(List<Integer> recordColList){
        int index = 0;
        recordColList.add(index++, 0);      // inmate_id
        recordColList.add(index++, 3);      // offense general
        recordColList.add(index++, 4);      // education
        recordColList.add(index++, 5);      // admit year
        recordColList.add(index++, 10);     // sentence length
        recordColList.add(index++, 11);     // offense detail
        recordColList.add(index++, 12);     // race
        recordColList.add(index++, 13);     // age admitted
        recordColList.add(index++, 15);     // times served
        recordColList.add(index++, 16);     // reltype
        recordColList.add(index++, 17);     // state
    }
}
