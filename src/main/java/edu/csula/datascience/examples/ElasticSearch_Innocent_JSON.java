package edu.csula.datascience.examples;

import com.google.gson.Gson;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Date;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticSearch_Innocent_JSON {
    private boolean DEBUG = true;
    private long myTime = new Date().getTime();

    private final String indexName = "project-data";

    private final String executions_typeName = "executions";
    private final String exonerations_typeName = "exonerations";
    private final String term_records_typeName = "term_records";
    private final String salaries_typeName = "salaries";

    // file names
    private final String term_records_fileName = "Term_Records.json";
    private final String executions_fileName = "Executions.json";
    private final String exonerated_fileName = "Exonerated.json";
//    private final String salaries_fileName = "Salaries.csv";

    public static void main(String[] args) throws URISyntaxException{
        ElasticSearch_Innocent_JSON tester = new ElasticSearch_Innocent_JSON();
        tester.driver();
    }

    public void driver() throws URISyntaxException{

        Node node = nodeBuilder().settings(Settings.builder()
                .put("cluster.name", "real-tazzie")
                .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        //getData(client, salaries_fileName, salaries_typeName);
        getData(client, executions_fileName, executions_typeName);
        //getData(client, exonerated_fileName, exonerations_typeName);
        //getData(client, term_records_fileName, term_records_typeName);

        myTime = new Date().getTime() - myTime;
        System.out.println("Program ran for\n" + myTime + " ms"
                + "\nor " + myTime/1000 + " seconds"
                + "\nor " + myTime/(1000*60) + " minutes"
                + "\nor " + myTime/(1000*60*60) + " hours");
    }

    private void getData(Client client, String fileName, String typeName) throws URISyntaxException{
        /**
         *
         *
         * INSERT data to elastic search
         */

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        File json_file = new File(
                ClassLoader.getSystemResource(fileName)
                        .toURI()
        );

        int fileNum = 0;

        // create bulk processor
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println("Facing error while importing data to elastic search");
                        failure.printStackTrace();
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        // Gson library for sending json to elastic search
        Gson gson = new Gson();

        try {
            BufferedReader br = new BufferedReader(new FileReader(json_file));
            String line;
            boolean firstLine = true;
            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                JSONObject json_object = new JSONObject(line);

                switch(fileName){
                    case exonerated_fileName:
                        System.out.println("RecordNum: " + json_object.get("Id") + "\tState: "
                                + json_object.get("State")
                        );
                        Exonerated exonerate = new Exonerated(
                                json_object.get("Id").toString(),
                                json_object.get("Race").toString(),
                                json_object.get("State").toString(),
                                json_object.get("Worst Crime Display").toString(),
                                Integer.parseInt(json_object.get("Convicted").toString()),
                                Integer.parseInt(json_object.get("Exonerated").toString()),
                                json_object.get("Sentence").toString(),
                                json_object.get("OM").toString(),
                                json_object.get("Region").toString()
                        );
                        bulkProcessor.add(new IndexRequest(indexName, typeName)
                                .source(gson.toJson(exonerate))
                        );
                        break;
                    case executions_fileName:
                        System.out.println("Date/Race/State: "
                                + json_object.get("Year").toString()
                                + "/" + json_object.get("Race").toString()
                                + "/" + json_object.get("State").toString()
                                + "/" + json_object.get("Region").toString()
                        );
                        Execution execute = new Execution(
                                Integer.parseInt(json_object.get("Year").toString()),
                                Integer.parseInt(json_object.get("Age").toString()),
                                json_object.get("Race").toString(),
                                json_object.get("State").toString(),
                                json_object.get("Crime").toString(),
                                json_object.get("Region").toString()
                        );
                        bulkProcessor.add(new IndexRequest(indexName, typeName)
                                .source(gson.toJson(execute))
                        );
                        break;
                    default:
                        // place holder for term_records
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Double parseSafe(String value) {
        return Double.parseDouble(value.isEmpty() || value.equals("Not Provided") ? "0" : value);
    }

    class Execution {
        final int date;
        final int age;
        final String race;
        final String state;
        final String murder;
        final String region;

        public Execution(int date, int age, String race, String state, String murder, String region) {
            this.date = date;
            this.age = age;
            this.race = race;
            this.state = state;
            this.murder = murder;
            this.region = region;
        }
    }

    class Exonerated {
        final String id;
        final String race;
        final String state;
        final String crime;
        final int convicted_year;
        final int exonerated_year;
        final String sentence;
        final String om;
        final String region;

        public Exonerated(String id,String race, String state, String crime, int convicted_year, int exonerated_year, String sentence,String om,String region) {
            this.id = id;
            this.race = race;
            this.state = state;
            this.crime = crime;
            this.convicted_year = convicted_year;
            this.exonerated_year = exonerated_year;
            this.sentence = sentence;
            this.om = om;
            this.region = region;
        }
    }

    class Salary {
        private final long id;
        private final String name;
        private final String jobTitle;
        private final double basePay;
        private final double overtimePay;
        private final double otherPay;
        private final double benefits;
        private final double totalPay;
        private final double totalPayBenefits;
        private final int year;
        private final String notes;
        private final String agency;
        private final String status;

        public Salary(long id, String name, String jobTitle, double basePay, double overtimePay, double otherPay, double benefits, double totalPay, double totalPayBenefits, int year, String notes, String agency, String status) {
            this.id = id;
            this.name = name;
            this.jobTitle = jobTitle;
            this.basePay = basePay;
            this.overtimePay = overtimePay;
            this.otherPay = otherPay;
            this.benefits = benefits;
            this.totalPay = totalPay;
            this.totalPayBenefits = totalPayBenefits;
            this.year = year;
            this.notes = notes;
            this.agency = agency;
            this.status = status;
        }
    }
//    class TermRecord {
//
//        public TermRecord(String id, String inmate_id, String sex, String admit_type,
//                          String offense_general, String admit_year, String mandatory_release_year,
//                          String projected_release_year, String parole_year, String SENTLGTH	OFFDETAIL	RACE	AGEADMIT	AGERELEASE	TIMESRVD	RELTYPE	STATE	REGION
//        ) {
//        }
//    }
}
