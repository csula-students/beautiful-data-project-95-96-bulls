package edu.csula.datascience.acquisition;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Tazzie on 5/1/2016.
 */
public class InnocentCollector implements Collector<String, String> {
    private boolean DEBUG = true;

    public void driver() throws URISyntaxException, IOException {
        if (DEBUG)
            System.out.println("Running InnocentCollector");

        // read file and extract info from file
        getData("Term_Records_1991_to_2014.tsv");

        // store info

    }

    private void getData(String filename) throws URISyntaxException, IOException {

        File tsv = new File(
                ClassLoader.getSystemResource(filename)
                        .toURI()
        );

        StringTokenizer st;
        BufferedReader TSVFile = new BufferedReader(new FileReader(tsv));
        List<String> dataArray = new ArrayList<String>() ;
        String dataRow = TSVFile.readLine(); // Read first line.
        int counter = 0;

        // total records = 10907334
        // counter sentinel must be <= 9000000 to store in arraylist without being out of memory
        while (dataRow != null && counter < 9000000){
//            System.out.println("Counter: " + counter);
            st = new StringTokenizer(dataRow,"\\t");
            while(st.hasMoreElements()){
//                System.out.println("st still has more elements");
                dataArray.add(st.nextElement().toString());
            }
            dataRow = TSVFile.readLine(); // Read next line of data.

            counter++;
        }
        // Close the file once all data has been read.
        TSVFile.close();

//        System.out.println("done with tokens");
//        System.out.println("traversing dataArray");
//        System.out.println();

        if (DEBUG) {
            for (String item : dataArray) {
                System.out.println(item); // Print the data line.
            }
            System.out.println("done with dataArray");
        }

        System.out.println("Total records: " + counter);

        // End the printout with a blank line.
        System.out.println();
    }

    @Override
    public Collection<String> mungee (Collection <String> src){
        return src;
    }

    @Override
    public void save(Collection<String> data){

    }

    public static void main(String[] args) throws URISyntaxException, IOException {
        InnocentCollector tester = new InnocentCollector();
        tester.driver();
    }

}
