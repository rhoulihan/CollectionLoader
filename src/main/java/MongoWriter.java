package com.mongodb.CollectionLoader;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import com.mongodb.client.MongoCollection;

public class MongoWriter implements Runnable {

	private Logger logger;
	private int threadId;
	private MongoCollection<Document> coll;
	private List<Document> docs;

	public MongoWriter(int threadid, MongoCollection<Document> coll, List<Document> docs) {
		logger = LoggerFactory.getLogger(MongoWriter.class);

		this.threadId = threadid;
		this.coll = coll;
		this.docs = docs;
	}

	public void run() {
		try {
		logger.info(String.format("Starting Writer %d", threadId));

		coll.insertMany(docs);

		logger.info(String.format("Documents written by Writer %d: %d", threadId, docs.size()));
		} finally {
			synchronized (Main.numThreads) {
				if (Main.numThreads.decrementAndGet() == 0)
					Main.numThreads.notifyAll();
			}
		}
	}
}
