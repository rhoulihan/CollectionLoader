# CollectionLoader

This is a Maven project.

To build install Maven, open a shell, navigate to the root of the project and run the following command:

mvn clean package shade:shade

To exewcute navigate to the target directory created by the build process and run the following command:

java -jar CollectionLoader.jar -?

Usage: java -jar TableLoader.jar [options]
-n  <number>            Number of customers
-m  <number>            Maximum number of orders per customer
-i  <number>            Maximum number of items per order
-p  <number>            Number of products
-l                      Skip table loading
-u <string>             MongoDB URI