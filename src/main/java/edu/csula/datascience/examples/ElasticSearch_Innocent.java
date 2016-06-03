package edu.csula.datascience.examples;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
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
public class ElasticSearch_Innocent {
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


        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
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
                // cleaning up dirty data which doesn't have time or temperature
                if (
                        !record.get("Race").isEmpty()
                        ) {
                    Execution temp = new Execution(
                            record.get("Date"),
                            record.get("Race"),
                            record.get("State")
                    );

                    bulkProcessor.add(new IndexRequest(indexName, executions_typeName)
                            .source(gson.toJson(temp))
                    );
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        /**
         * Structured search
         */

        // simple search by field name "state" and find Washington
//        SearchResponse response = client.prepareSearch(indexName)
//            .setTypes(typeName)
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
        SearchResponse sr = node.client().prepareSearch(indexName)
                .setTypes(executions_typeName)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms("raceAgg").field("Race")
                                .size(Integer.MAX_VALUE)
                )
                .execute().actionGet();

        // Get your facet results
        Terms agg1 = sr.getAggregations().get("raceAgg");

        for (Terms.Bucket bucket: agg1.getBuckets()) {
            System.out.println(bucket.getKey() + ": " + bucket.getDocCount());
        }
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

}
