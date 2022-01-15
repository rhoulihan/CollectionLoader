# CollectionLoader

This is a Maven project.

To build install Maven, open a shell, navigate to the root of the project and run the following command:

mvn clean package shade:shade

To exewcute navigate to the target directory created by the build process and run the following command:

java -jar CollectionLoader.jar -?

Usage: java -jar TableLoader.jar [options]<br/>
&ensp;-n&ensp;<number>&emsp;&emsp;Number of customers<br/>
&ensp;-m&ensp;<number>&emsp;&emsp;Maximum number of orders per customer<br/>
&ensp;-i&ensp;<number>&emsp;&emsp;Maximum number of items per order<br/>
&ensp;-p&ensp;<number>&emsp;&emsp;Number of products<br/>
&ensp;-l&ensp;&emsp;&emsp;&emsp;&emsp;Skip table loading<br/>
&ensp;-u&ensp;<string>&emsp;&emsp; MongoDB URI<br/>
