package edu.csula.datascience.examples;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * A quick elastic search example app
 *
 * It will parse the csv file from the resource folder under main and send these
 * data to elastic search instance running locally
 *
 * After that we will be using elastic search to do full text search
 *
 * gradle command to run this app `gradle esExample`
 */
public class ElasticSearch_Innocent2 {
    private final String indexName = "project-data";
    private final String executions_typeName = "executions";
    private final String exonerations_typeName = "exoneration";
    private final String terms_typeName = "terms";
    private final String executions_fileName = "Executions_1986_to_2016.csv";
    private final String exonerations_fileName = "Executions_1986_to_2016.csv";
    private final String terms_fileName = "Term_Records_1991_to_2014.tsv";

    public static void main(String[] args) throws URISyntaxException, IOException {
        ElasticSearch_Innocent tester = new ElasticSearch_Innocent();
        tester.driver();
    }

    public void driver() throws URISyntaxException, IOException {
        Node node = nodeBuilder().settings(Settings.builder()
                .put("cluster.name", "real-tazzie")
                .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        // Try to aggregate
        Terms agg1 = null;

        boolean exists = client.admin().indices()
                .prepareExists(indexName)
                .execute().actionGet().isExists();

        if (exists){
            try{
                System.out.println("Trying to aggregate");
                // Get your facet results
                agg1 = getAggregate(node, "raceAgg", "Race");
            }
            catch(Exception e){
                System.err.println("Failed to aggregate. Generating new index.");
                // creating index to elastic search
                createElasticIndex(client);
                // Get your facet results
                agg1 = getAggregate(node, "raceAgg", "Race");
            }
        }
        /**
         * Structured search
         */

        // simple search by field name "state" and find Washington
//        SearchResponse response = client.prepareSearch(indexName)
//            .setTypes(executions_typeName)
//            .setSearchType(SearchType.DEFAULT)
//            .setQuery(QueryBuilders.matchQuery("state", "Washington"))   // Query
//            .setScroll(new TimeValue(60000))
//            .setSize(60).setExplain(true)
//            .execute()
//            .actionGet();
//
//        //Scroll until no hits are returned
//        while (true) {
//
//            for (SearchHit hit : response.getHits().getHits()) {
//                System.out.println(hit.sourceAsString());
//            }
//            response = client
//                .prepareSearchScroll(response.getScrollId())
//                .setScroll(new TimeValue(60000))
//                .execute()
//                .actionGet();
//            //Break condition: No hits are returned
//            if (response.getHits().getHits().length == 0) {
//                break;
//            }
//        }
//
        /**
         * AGGREGATION
         */

        if (exists) {
            for (Terms.Bucket bucket : agg1.getBuckets()) {
                System.out.println(bucket.getKey() + ": " + bucket.getDocCount());
            }
        }
        else
            System.out.println("Failed to aggregate. Please try again.");
    }
    public class Execution {
        final String date;
        final String race;
        final String state;

        public Execution(String date, String race, String state) {
            this.date = date;
            this.race = race;
            this.state = state;
        }
    }

    // creates new index
    private void createElasticIndex(Client client) throws URISyntaxException, IOException {
        System.out.println("In createElasticIndex");
        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        setData(client);


    }

    // generates data to be used for index of elastic search
    private void setData(Client client) throws URISyntaxException, IOException {
        File csv = new File(
                ClassLoader.getSystemResource(executions_fileName)
                        .toURI()
        );

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
            // after reading the csv file, we will use CSVParser to parse through
            // the csv files
            CSVParser parser = CSVParser.parse(
                    csv,
                    Charset.defaultCharset(),
                    CSVFormat.EXCEL.withHeader()
            );

            // for each record, we will insert data into Elastic Search

            parser.forEach(record -> {
                // cleaning up dirty data
                if (!record.get("Race").isEmpty()) {
                    System.out.println("Setting execute");
                    Execution temp = new Execution(
                            record.get("Date"),
                            record.get("Race"),
                            record.get("State")
                    );

                    System.out.println("Done execute, doing bulkProcess");

                    bulkProcessor.add(new IndexRequest(indexName, executions_typeName)
                            .source(gson.toJson(temp))
                    );

//                    addRecord(client, "Date", record.get("Date"));
//                    addRecord(client, "Race", record.get("Race"));
//                    addRecord(client, "State", record.get("State"));
                }
            });
        } catch (IOException e) {
            System.out.println("Failed parse data in function setData");
            e.printStackTrace();
        }
    }

    private void addRecord(Client client, String field_name, String field_value){
        System.out.println("Adding " + field_name);
        Settings settings = Settings.settingsBuilder()
                .put(field_name, field_value)
                .build();
        CreateIndexRequest indexRequest = new CreateIndexRequest(indexName, settings);
        client.admin().indices().create(indexRequest).actionGet();

    }
    // returns an aggregate
    private Terms getAggregate(Node node, String term, String field){
        SearchResponse sr = node.client().prepareSearch(indexName)
                .setTypes(executions_typeName)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms(term)
                                .size(Integer.MAX_VALUE)
                                .field(field)
                )
                .execute().actionGet();
        return sr.getAggregations().get(term);
    }
}
