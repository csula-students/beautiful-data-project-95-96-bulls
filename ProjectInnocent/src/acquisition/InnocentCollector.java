package acquisition;

import java.util.Collection;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class InnocentCollector implements Collector<String, String>{

	@Override
	public Collection<String> mungee(Collection<String> src) {
	    MongoClient mongoClient;
	    MongoDatabase database;
	    MongoCollection<Document> collection;
		return null;
	}

	@Override
	public void save(Collection<String> data) {
		// TODO Auto-generated method stub
		
	}

}
