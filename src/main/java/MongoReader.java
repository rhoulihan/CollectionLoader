package com.mongodb.CollectionLoader;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class MongoReader implements Runnable {
	private String orderId;
	private MongoCollection<Document> coll;

	public MongoReader(MongoCollection<Document> coll, String orderId) {
		this.coll = coll;
		this.orderId = orderId;
	}

	public void run() {
		MongoCursor<Document> cursor = null;
		Document query = new Document("_id", new Document("$gte", orderId));
		
		try {
			cursor = coll.find(query).iterator();
			while (cursor.hasNext())
				cursor.next();
		} finally {
			synchronized (Main.numThreads) {
				if (Main.numThreads.decrementAndGet() == 0)
					Main.numThreads.notifyAll();
			}
			cursor.close();
		}
	}
}
