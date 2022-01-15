# CollectionLoader

This is a Maven project.

To build install Maven, open a shell, navigate to the root of the project and run the following command:

mvn clean package shade:shade

To exewcute navigate to the target directory created by the build process and run the following command:

java -jar CollectionLoader.jar -?

Usage: java -jar TableLoader.jar [options]<br/>
-n  <number>            Number of customers<br/>
-m  <number>            Maximum number of orders per customer<br/>
-i  <number>            Maximum number of items per order<br/>
-p  <number>            Number of products<br/>
-l                      Skip table loading<br/>
-u  <string>            MongoDB URI<br/>
