This is hadoop program to get me familiar with programming MapReduce in java.
The function it implemented is simple:

for cookie.csv like following:

>cookie,date
>1000,130421
>1001,130420
>1001,130421

implement functionality like:

    SELECT date, COUNT(DISTINCT cookie), COUNT(cookie) FROM table GROUP BY date;

Usage
=====

use following command to try:

    $ hadoop fs -mkdir input
    $ hadoop fs -put cookie.csv input
    $ javac CookieCount.java -classpath path/to/hadoop_home/hadoop-core-*.jar
    $ jar -cf ~/CookieCount.jar *.class
    $ hadoop jar ~/CookieCount.jar CookieCount input output

see result like:

    $ hadoop fs -cat output/part*

Having fun.
