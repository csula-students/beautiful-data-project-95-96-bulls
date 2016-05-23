
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
 *
 */
public class InnocentCollectorApp {
    private boolean DEBUG = true;

    // MongoDB variables
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private int mongoIndex = 0;

    // file names
    String term_tsv = "Term_Records_1991_to_2014.tsv";
    String executions_csv = "Executions_1986_to_2016.csv";
    String exhonorated_csv = "Exonorated_1989_to_2016.csv";


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

        // we want to update collection with new data, so drop old one
        collection.drop();

/*        Document stuff = new Document("address", "my house")
                .append("borough", "Manhattan")
                .append("cuisine", "Italian")
                .append("grades", "A")
                .append("name", "Vella")
                .append("restaurant_id", "41704620");

        collection.insertOne(stuff);*/

        // read file and extract info from file
        getData(executions_csv);
        getData(exhonorated_csv);
        getData(term_tsv);


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

        // loop to traverse through file(s)
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

            // exhonorated handling
            if (filename.equals(exhonorated_csv))
                addExhonoratedToMongo(dataRowArray, mongoIndex);

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
//        Document info = new Document("ABT_INMATE_ID", rowDataArray[0])
//                .append("SEX", rowDataArray[1])
//                .append("ADMTYPE", rowDataArray[2])
//                .append("OFFGENERAL", rowDataArray[3])
//                .append("EDUCATION", rowDataArray[4])
//                .append("ADMITYR", rowDataArray[5])
//                .append("RELEASEYR", rowDataArray[6])
//                .append("MAND_PRISREL_YEAR", rowDataArray[7])
//                .append("PROJ_PRISREL_YEAR", rowDataArray[8])
//                .append("PARELIG_YEAR", rowDataArray[9])
//                .append("SENTLGTH", rowDataArray[10])
//                .append("OFFDETAIL", rowDataArray[11])
//                .append("RACE", rowDataArray[12])
//                .append("AGEADMIT", rowDataArray[13])
//                .append("AGERELEASE", rowDataArray[14])
//                .append("TIMESRVD", rowDataArray[15])
//                .append("RELTYPE", rowDataArray[16])
//                .append("STATE", rowDataArray[17]);
        Document info = new Document("RACE", rowDataArray[12])
                .append("filename", term_tsv);

        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("record_id", index)
                .append("info", info);

        // to insert document
        collection.insertOne(info);
    }

    private void addExecutionsToMongo(String[] rowDataArray, int index){
        // to create new document
        Document info = new Document("RACE", rowDataArray[4])
                .append("filename", executions_csv);

        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("record_id", index)
                .append("info", info);

        // to insert document
        collection.insertOne(info);
    }

    private void addExhonoratedToMongo(String[] rowDataArray, int index){
        // to create new document
        Document info = new Document("RACE", rowDataArray[3])
                .append("filename", exhonorated_csv);

        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("record_id", index)
                .append("info", info);

        // to insert document
        collection.insertOne(info);
    }
}

