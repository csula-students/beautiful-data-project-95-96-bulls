package edu.csula.datascience.acquisition;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;


/**
 * Created by Tazzie on 5/1/2016.
 */
public class InnocentCollector implements Collector<String, String> {
    private boolean DEBUG = true;
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> collection;

    public InnocentCollector() {
        // establish database connection to MongoDB
        mongoClient = new MongoClient();

        // select `bd-example` as testing database
        database = mongoClient.getDatabase("bd-example");

        // select collection by name `tweets`
        collection = database.getCollection("records");
    }

    @Override
    public Collection<String> mungee (Collection <String> src){
        return src;
    }

    @Override
    public void save(Collection<String> data){

    }


}
