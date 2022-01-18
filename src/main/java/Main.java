package com.mongodb.CollectionLoader;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.LogManager;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;

public class Main {
	// misc globals
	public static volatile AtomicInteger numThreads = new AtomicInteger(0);

	public static ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(60);
	public static int count = 0;
	public static boolean runFlag = true;

	private static long elapsed;
	private static Map<String, Integer> counts = new HashMap<String, Integer>();
	private static String demo = "shootout";
	private static List<String> keys = new ArrayList<String>();
	private static boolean loadItems = true;
	private static Random random = new Random();
	private static Calendar cal = Calendar.getInstance();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private static boolean mongodb = false;
	private static Map<String, MongoCollection<Document>> collections = new HashMap<String, MongoCollection<Document>>();
	private static com.mongodb.client.MongoClient mClient;
	static Logger logger;
	private static List<Document> documents = new ArrayList<Document>(), embedded = new ArrayList<Document>();
	private static List<Document> products = new ArrayList<Document>();
	private static List<String> orderIds = new ArrayList<String>();
	private static String orderId;
	private static MongoDatabase mdb;
	private static String mongoUri;
	private static Map<String, List<Document>> docs;

	private static Document order;

	private static List<Document> items = new ArrayList<Document>();

	private static int size = 6400;

	// main function
	public static void main(String[] args) {
		LogManager.getLogManager().reset();
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		logger = LoggerFactory.getLogger(Main.class);

		Scanner scanner = new Scanner(System.in);
		disableWarning();

		// set globals
		parseArgs(args);

		if (mongodb) {
			mClient = MongoClients.create(
					MongoClientSettings.builder().applyConnectionString(new ConnectionString(mongoUri)).build());
			mdb = mClient.getDatabase("perftest");

			mdb.getCollection("data").drop();
			mdb.getCollection("embedded").drop();
			mdb.getCollection("customer").drop();
			mdb.getCollection("order").drop();
			mdb.getCollection("orderItem").drop();
			mdb.getCollection("invoice").drop();
			mdb.getCollection("payment").drop();
			mdb.getCollection("shipment").drop();
			mdb.getCollection("warehouse").drop();

			mdb.createCollection("data");
			mdb.createCollection("embedded");
			mdb.createCollection("customer");
			mdb.createCollection("order");
			mdb.createCollection("orderItem");
			mdb.createCollection("invoice");
			mdb.createCollection("payment");
			mdb.createCollection("shipment");
			mdb.createCollection("warehouse");

			collections.put("data", mdb.getCollection("data"));
			collections.put("embedded", mdb.getCollection("embedded"));
			collections.put("customer", mdb.getCollection("customer"));
			collections.put("order", mdb.getCollection("order"));
			collections.put("orderItem", mdb.getCollection("orderItem"));
			collections.put("invoice", mdb.getCollection("invoice"));
			collections.put("payment", mdb.getCollection("payment"));
			collections.put("shipment", mdb.getCollection("shipment"));
			collections.put("warehouse", mdb.getCollection("warehouse"));
		}

		if (loadItems) {
			Map<String, String> params = new HashMap<String, String>();

			params.put("address",
					"{\"Country\":\"Sweden\",\"County\":\"Vastra Gotaland\",\"City\":\"Goteborg\",\"Street\":\"MainStreet\",\"Number\":20,\"ZipCode\":41111}");

			loadItems("warehouse", 1, params);

			params.put("address",
					"{\"Country\":\"Sweden\",\"County\":\"Vastra Gotaland\",\"City\":\"Boras\",\"Street\":\"RiverStreet\",\"Number\":20,\"ZipCode\":11111}");

			loadItems("warehouse", 1, params);

			params.clear();
			loadItems("product", counts.get("products"), params);
			params.clear();
			loadItems("customer", counts.get("customers"), params);

			drainQueue();

			System.out.println("Hit [ENTER] to continue...");
			scanner.nextLine();
		}

		// Prewarm thread pool
		System.out.println("Prewarming thread pool...");
		tpe.prestartAllCoreThreads();
		getAllOrdersById("multi");

		// Start the test
		long sTime = 0L, mTime = 0L, eTime = 0L, single, multi, embed;

		for (int i = 0; i < 10; i++) {
			System.out.println(String.format("\nIteration %d:", i));

			// Run getOrderById test and record execution times for each model
			synchronized (numThreads) {
				// Multiple Collection
				multi = runGetAllOrdersByIdTest("multi");

				// Single Collection
				single = runGetAllOrdersByIdTest("data");

				// Embedded Document
				embed = runGetAllOrdersByIdTest("embedded");
			}

			// Report Single table efficiency as a percentage of Multi and Embedded model
			// response times
			System.out.println(
					String.format("Single vs Multiple Collection efficiency: %d%s", (single * 100) / (multi), "%"));
			System.out.println(
					String.format("Single vs Embedded Document efficiency: %d%s", (single * 100) / (embed), "%"));

			mTime += multi;
			sTime += single;
			eTime += embed;
		}

		// Report average efficiency over N iterations
		System.out.println();
		System.out.println(String.format("Average Multiple Collection load time %dms", mTime / 10));
		System.out.println(String.format("Average Single Collection load time %dms", sTime / 10));
		System.out.println(String.format("Average Embedded Document load time %dms", eTime / 10));
		System.out.println();
		System.out.println(
				String.format("Average Single vs. Multiple Collection efficiency: %d%%", (sTime * 100) / (mTime)));
		System.out.println(
				String.format("Average Single vs. Embedded Document efficiency: %d%%", (sTime * 100) / (eTime)));

		// shutdown the thread pool and exit
		System.out.println("Shutting down....");
		scanner.close();
		try {
			tpe.shutdown();
			tpe.awaitTermination(5L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("Done.\n");
	}

	private static long runGetAllOrdersByIdTest(String collection) {
		count = 0;
		System.out.print(String.format("Running getOrderById test for %s data model...",
				collection.equals("data") ? "Single Collection"
						: collection.equals("multi") ? "Multiple Collection" : "Embedded Document"));
		elapsed = System.currentTimeMillis();

		switch (collection) {
		case "data":
		case "embedded":
			getAllOrdersById(collection);
			break;

		case "multi":
			for (String name : docs.keySet())
				getAllOrdersById(name);
			break;
		}

		try {
			synchronized (numThreads) {
				numThreads.wait();
			}
		} catch (InterruptedException e) {
		}

		elapsed = System.currentTimeMillis() - elapsed;
		System.out.println(String.format("\nRetrieved %d order objects in %dms,", orderIds.size(), elapsed));

		return elapsed;
	}

	private static void getAllOrdersById(String collection) {
		for (String orderId : orderIds) {
			synchronized (Main.numThreads) {
				numThreads.incrementAndGet();
			}
			tpe.execute(new MongoReader(mdb.getCollection(collection), orderId));
		}
	}

	private static int loadItems(String type, int qty, Map<String, String> params) {
		int ret = 0;
		elapsed = System.currentTimeMillis();

		if (!demo.equals("cci")) {
			String custId, timestamp;
			int amount;

			for (int itemCount = 0; itemCount < qty; itemCount++) {
				String pk, sk;

				switch (type) {
				case "customer":
					pk = String.format("C#%d", counts.put("customers", counts.get("customers") + 1));
					params.put("customerId", pk);

					String email = String.format("%s@somewhere.com", getString(10));

					if (mongodb) {
						documents.add(new Document().append("_id", pk).append("type", "customer").append("email", email)
								.append("data", new String(new byte[random.nextInt(6400)], Charset.forName("UTF-8"))));
					}

					loadItems("order", random.nextInt((counts.get("orders") != null ? counts.get("orders") : 5)),
							params);
					break;

				case "order":
					pk = String.format("O#%d", counts.put("items", counts.get("items") + 1));
					cal.add(Calendar.DAY_OF_YEAR, random.nextInt(30) * -1);
					timestamp = sdf.format(cal.getTime());

					params.put("orderId", pk);

					order = new Document().append("_id", pk + "#").append("custId", params.get("customerId"))
							.append("type", "order").append("date", timestamp);
					
					items = new ArrayList<Document>();

					params.put("amount",
							Integer.toString(loadItems("orderItem", counts.get("orderItems"), params)));

					if (random.nextBoolean()) {
						orderId = pk;
						loadItems("invoice", 1, params);
						loadItems("shipment", 1, params);
					}

					if (mongodb) {
						documents.add(new Document().append("_id", pk + "#").append("custId", params.get("customerId"))
								.append("type", "order").append("date", timestamp)
								.append("amount", params.get("amount")));

						embedded.add(order.append("amount", params.get("amount")).append("items", items));

						orderIds.add(pk + "#");
					}

					cal = Calendar.getInstance();
					break;

				case "invoice":
					pk = params.get("orderId");
					sk = String.format("I#%d", counts.put("items", counts.get("items") + 1));
					cal.add(Calendar.DAY_OF_YEAR, 1);
					custId = params.get("customerId");
					timestamp = sdf.format(cal.getTime());
					amount = Integer.valueOf(params.get("amount"));

					if (mongodb) {
						Document invoice = new Document().append("_id", pk + "#" + sk).append("invoiceId", sk)
								.append("type", "invoice").append("date", timestamp).append("amount", amount)
								.append("custId", custId);

						documents.add(invoice);
						order.append("invoice", invoice);
					}
					break;

				case "orderItem":
					pk = params.get("orderId");
					timestamp = sdf.format(cal.getTime());
					custId = params.get("customerId");
					int orderQty = random.nextInt(5);

					if (mongodb) {
						Document pDoc = products.get(random.nextInt(products.size())), item = new Document()
								.append("_id", pk + "#" + itemCount).append("productId", pDoc.getString("_id"))
								.append("type", "orderItem").append("date", timestamp).append("custId", custId)
								.append("qty", qty).append("price", pDoc.getInteger("price"))
								.append("detail", pDoc.get("detail"))
								.append("data", new String(new byte[random.nextInt(size)], Charset.forName("UTF-8")));

						documents.add(item);
						items.add(item);

						ret += orderQty * pDoc.getInteger("price");
					}
					break;

				case "shipment":
					pk = params.get("orderId");
					sk = String.format("S#%d", counts.put("items", counts.get("items") + 1));

					params.put("shipmentId", sk);

					String key = keys.get(random.nextInt(keys.size())),
							method = (random.nextBoolean() ? "Express" : "Standard");
					timestamp = sdf.format(cal.getTime());

					params.put("warehouse", key);
					params.put("timestamp", timestamp);

					if (mongodb) {
						Document shipment = new Document().append("_id", pk + "#" + sk).append("shipmentId", sk)
								.append("type", "shipment").append("date", timestamp)
								.append("shipTo",
										new Document().append("Country", "Sweden").append("County", "Vastra Gotaland")
												.append("City", "Goteborg").append("Street", "Slanbarsvagen")
												.append("Number", 34).append("ZipCode", 41787))
								.append("method", method);

						documents.add(shipment);
						order.append("shipment", shipment);
					}

					// loadItems("shipItem", results.get(1).size(), params);
					break;

				case "shipItem":
					pk = params.get("orderId");
					sk = String.format("SI#%d", counts.put("items", counts.get("items") + 1));

					if (mongodb) {
						for (Document doc : documents) {
							if (doc.getString("_id").startsWith(orderId) && doc.getString("type").equals("orderItem"))
								doc.append("shipmentId", params.get("shipmentId"));
						}
					}
					break;

				case "warehouse":
					pk = String.format("W#%d", counts.put("items", counts.get("items") + 1));
					keys.add(pk);

					if (mongodb) {
						documents.add(new Document().append("_id", pk).append("type", "warehouse").append("address",
								params.get("address")));
					}
					break;

				case "product":
					pk = String.format("P#%d", counts.put("items", counts.get("items") + 1));
					sk = keys.get(random.nextInt(keys.size()));

					if (mongodb) {
						products.add(new Document().append("_id", pk).append("warehouseId", sk).append("detail",
								new Document().append("Name", "Product" + counts.get("items")).append("Description",
										new String(new byte[random.nextInt(50)], Charset.forName("UTF-8"))))
								.append("qty", random.nextInt(100) + 100).append("price", random.nextInt(50) + 10));
					}
					break;
				}
			}
		}
		return ret;
	}

	private static void drainQueue() {
		elapsed = System.currentTimeMillis();

		if (mongodb) {
			synchronized (Main.numThreads) {
				tpe.execute(new MongoWriter(numThreads.incrementAndGet(), mdb.getCollection("data"), documents));
				tpe.execute(new MongoWriter(numThreads.incrementAndGet(), mdb.getCollection("embedded"), embedded));
			}

			docs = new HashMap<String, List<Document>>();
			for (Document doc : documents) {
				docs.putIfAbsent(doc.getString("type"), new ArrayList<Document>());

				docs.get(doc.getString("type")).add(doc);
			}

			synchronized (Main.numThreads) {
				for (String name : docs.keySet()) {
					tpe.execute(new MongoWriter(numThreads.incrementAndGet(), mdb.getCollection(name), docs.get(name)));
				}
			}

			try {
				synchronized (numThreads) {
					numThreads.wait();
				}
			} catch (InterruptedException e) {
			}
		}
	}

	private static String getString(int length) {
		String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		StringBuilder string = new StringBuilder();

		while (string.length() < length)
			string.append(chars.charAt(random.nextInt(chars.length())));

		return string.toString();
	}

	private static void parseArgs(String[] args) {
		String last = "";
		Map<String, String> argVals = new HashMap<String, String>();
		for (String arg : args) {
			if (arg.startsWith("-")) {
				if (argVals.putIfAbsent(arg, "") != null)
					usage(String.format("ERROR: Duplicate argument [%s].", arg));

				last = arg;
			} else {
				if (last.equals(""))
					usage(String.format("ERROR: Unable to associate argument value [%s]", arg));
				else {
					argVals.put(last, arg);
					last = "";
				}
			}
		}

		for (String key : argVals.keySet()) {
			switch (key) {
			case "-s":
				size  = Integer.parseInt(argVals.get(key));
				break;
				
			case "-u":
				mongodb = true;
				mongoUri = argVals.get(key);
				break;

			case "-d":
				demo = argVals.get(key);
				break;

			case "-i":
				counts.put("orderItems", Integer.valueOf(argVals.get(key)));
				counts.put("items", Integer.valueOf(argVals.get(key)));
				break;

			case "-m":
				counts.put("orders", Integer.valueOf(argVals.get(key)));
				break;

			case "-n":
				counts.put("customers", Integer.valueOf(argVals.get(key)));
				break;

			case "-p":
				counts.put("products", Integer.valueOf(argVals.get(key)));
				break;

			default:
				usage(String.format("ERROR: Unknown argument [%s].", key));
				break;
			}
		}
	}

	private static void usage(String message) {
		logger.error(message);
		System.out.println("Usage: java -jar TableLoader.jar [options]");
		System.out.println("-n  <number>\t\tNumber of customers");
		System.out.println("-m  <number>\t\tMaximum number of orders per customer");
		System.out.println("-i  <number>\t\tNumber of items per order");
		System.out.println("-p  <number>\t\tNumber of products");
		System.out.println("-s  <number>\t\tMaximum size of each order item history in bytes");
		System.out.println("-u <string> \t\tMongoDB URI");
		System.exit(1);
	}

	private static void disableWarning() {
		try {
			Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			sun.misc.Unsafe u = (sun.misc.Unsafe) theUnsafe.get(null);

			Class<?> cls = Class.forName("jdk.internal.module.IllegalAccessLogger");
			Field logger = cls.getDeclaredField("logger");
			u.putObjectVolatile(cls, u.staticFieldOffset(logger), null);
		} catch (Exception e) {
			// ignore
		}
	}
}
