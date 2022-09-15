package ru.neoflex.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

public class Main {

    static Logger logger = Logger.getLogger("Main");

    static SparkConf sparkConf;

    private static List<Client> clientList = Arrays.asList(
            new Client("100", "Ivan"),
            new Client("200", "Peter"),
            new Client("300", "Oleg")
    );

    private static List<Order> orderList = Arrays.asList(
            new Order("1", BigDecimal.valueOf(120), "100", LocalDate.of(2022, 8, 1)),
            new Order("2", BigDecimal.valueOf(20), "200", LocalDate.of(2022, 9, 10)),
            new Order("3", BigDecimal.valueOf(340), "100", LocalDate.of(2022, 7, 1)),
            new Order("4", BigDecimal.valueOf(12), "100", LocalDate.of(2022, 9, 3)),
            new Order("5", BigDecimal.valueOf(33), "300", LocalDate.of(2022, 9, 7)),
            new Order("6", BigDecimal.valueOf(45), "100", LocalDate.of(2022, 9, 1)),
            new Order("7", BigDecimal.valueOf(45), "100", LocalDate.of(2022, 9, 11)),
            new Order("8", BigDecimal.valueOf(45), "100", LocalDate.of(2022, 9, 12)),
            new Order("9", BigDecimal.valueOf(45), "100", LocalDate.of(2022, 9, 13))
    );

    public static void main(String[] args) throws InterruptedException {
        sparkConf = new SparkConf();
        sparkConf.setAppName("SimpleJoin");
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.executor.memory","1g");
        //externalTest();
        //datasetJoin();
        streamJoin();
    }

    public static void externalTest() throws InterruptedException {
        //StreamingExamples.setStreamingLogLevels();
        SparkConf sparkConf = new SparkConf().setAppName("JavaQueueStream").setMaster("local[*]").set("spark.executor.memory","1g");

        // Create the context
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        // Create the queue through which RDDs can be pushed to
        // a QueueInputDStream

        // Create and push some RDDs into the queue
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }

        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
        for (int i = 0; i < 30; i++) {
            rddQueue.add(ssc.sparkContext().parallelize(list));
        }

        // Create the QueueInputDStream and use it do some processing
        JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
        JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(
                i -> new Tuple2<>(i % 10, 1));
        JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(
                (i1, i2) -> i1 + i2);

        reducedStream.print();
        ssc.start();
        ssc.awaitTermination();
    }

    public static void streamJoin() {

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        JavaRDD<Client> clients = streamingContext.sparkContext()
                .parallelize(clientList).setName("clients");
        JavaRDD<Order> orders = streamingContext.sparkContext()
                .parallelize(orderList).setName("orders");


        var clientSchema = new StructType()
                .add("id", DataTypes.StringType)
                .add("name", DataTypes.StringType);

        var orderSchema = new StructType()
                .add("number", DataTypes.StringType)
                .add("client", DataTypes.StringType)
                .add("amount", DataTypes.createDecimalType())
                .add("date", DataTypes.DateType);

        var joinedSchema = new StructType()
                .add("number", DataTypes.StringType)
                .add("client", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("date", DataTypes.DateType)
                .add("name", DataTypes.DateType);

        var monthlyCountSchema = new StructType()
                .add("client", DataTypes.StringType)
                .add("month", DataTypes.StringType)
                .add("count", DataTypes.IntegerType);

        var monthlyAmountSchema = new StructType()
                .add("client", DataTypes.StringType)
                .add("month", DataTypes.StringType)
                .add("amount", DataTypes.createDecimalType());

        SparkSession session = SparkSession.builder().config(streamingContext.sparkContext().getConf()).getOrCreate();

        var clientsStream = streamingContext
                .queueStream( new LinkedList<>(Arrays.asList(clients)), true)
                .mapToPair(t ->
                     new Tuple2<>(t.id, new GenericRowWithSchema(new Object[]{t.id, t.name}, clientSchema))
                );
        var ordersStream = streamingContext
                .queueStream(new LinkedList<>(Arrays.asList(orders)), true)
                .mapToPair(t ->
                     new Tuple2<>(t.client, new GenericRowWithSchema(new Object[]{t.number, t.client, t.amount, t.date}, orderSchema))
                );

        var joined = ordersStream
                // соединяем потоки
                .join(clientsStream).mapValues(x ->  new GenericRowWithSchema(new Object[]{
                        x._1.getString(0), x._1.getString(1), x._1.getDecimal(2), x._1.getLocalDate(3), x._2.getString(1)
                }, joinedSchema))
                // избавляюсь от старого ключа
                .transform(x -> x.map(y -> y._2));

        var monthlyCount = joined.mapToPair(row -> new Tuple2<>(
                row.getString(4) + "_" + row.getLocalDate(3).getMonth().name(),row))
                .groupByKey()
                .map(v -> {
                    var count = StreamSupport.stream(v._2.spliterator(), false).count();
                    var clientAndMonth = v._1.split("_");
                    return new GenericRowWithSchema(new Object[]{
                            clientAndMonth[0], clientAndMonth[1], count
                    }, monthlyCountSchema);
                }).persist();

        var monthlyAmount = joined.mapToPair(row -> new Tuple2<>(
                        row.getString(4) + "_" + row.getLocalDate(3).getMonth().name(),row))
                .groupByKey()
                .map(v -> {
                    AtomicReference<BigDecimal> amount = new AtomicReference<>(BigDecimal.ZERO);
                    v._2.forEach( x -> {
                        amount.set(x.getDecimal(2).add(amount.get()));
                    });
                    var clientAndMonth = v._1.split("_");
                    return new GenericRowWithSchema(new Object[]{
                            clientAndMonth[0], clientAndMonth[1], amount
                    }, monthlyAmountSchema);
                }).persist();

        monthlyCount.print();

        monthlyAmount.print();

        streamingContext.start();

        streamingContext.ssc().awaitTermination();
    }

    public static void datasetJoin() {

        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> clientsSet = session.createDataFrame(clientList, Client.class);
        Dataset<Row> ordersSet = session.createDataFrame(orderList, Order.class);

        Dataset<Row> joined = ordersSet.join(clientsSet, clientsSet.col("id").equalTo(ordersSet.col("client")));

        joined.show();

    }
}
