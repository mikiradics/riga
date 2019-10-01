package open.data.lv.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.collection.immutable.Set;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Enters {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\hadoop");
        System.setProperty("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
        PropertyConfigurator.configure(System.getProperty("user.dir") + "\\conf\\log4j.properties");
        String masterUrl = "local[1]";
        SparkConf conf = new SparkConf()
                .setMaster(masterUrl)
                .setAppName("Riga public transport")
                .set("SPARK_HOME", System.getProperty("user.dir"))
                .set("SPARK_CONF_DIR", System.getProperty("user.dir") + "\\conf");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        Logger.getRootLogger().setLevel(Level.OFF);
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.setConf("spark.sql.caseSensitive", "true");

        readRealEventsDataAndCreateViews(sqlContext);
        readGTFSDataAndCreateViews(sqlContext);
        processGTFSDataBuildSchedules(sqlContext);
        processEventsData(sqlContext);
    }

    private static void readRealEventsDataAndCreateViews(SQLContext sqlContext) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        String valFile = "real/ValidDati23_11_18.txt";
        Dataset<Row> tickets = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("dateFormat", "dd.MM.yyyy HH:mm:ss")
                .csv(classLoader.getResource(valFile).getPath());
        tickets.createOrReplaceTempView("tickets");
        Dataset<Row> routes = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("nullValue", "NULL")
                .option("delimiter", ";")
                .option("dateFormat", "yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d1.csv").getPath());
        routes = routes.union(sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("nullValue", "NULL")
                .option("delimiter", ";")
                .option("dateFormat", "yyyy-MM-dd HH:mm:ss.SSS")
                .csv(classLoader.getResource("real/VehicleMessages20181123d2.csv").getPath()));
        routes.createOrReplaceTempView("routes");
        Dataset<Row> vehicles = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(classLoader.getResource("real/Vehicles.csv").getPath());
        vehicles.createOrReplaceTempView("vehicles");
        Dataset<Row> eventType = sqlContext.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(classLoader.getResource("real/SendingReasons.csv").getPath());
        eventType.createOrReplaceTempView("event_type");
    }

    private static void readGTFSDataAndCreateViews(SQLContext sqlContext) {
        ClassLoader classLoader = SparkAnalysis.class.getClassLoader();
        Dataset<Row> routes = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/routes.txt").getPath());
        routes.createOrReplaceTempView("gtfs_routes");
        Dataset<Row> stop_times = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/stop_times.txt").getPath());
        stop_times.createOrReplaceTempView("stop_times");
        Dataset<Row> stops = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/stops.txt").getPath());
        stops.createOrReplaceTempView("stops");
        Dataset<Row> trips = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "HH:mm:ss")
                .csv(classLoader.getResource("real/GTFS/trips.txt").getPath());
        trips.createOrReplaceTempView("trips");
        Dataset<Row> type = sqlContext.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", ",")
                .csv(classLoader.getResource("real/GTFS/route_types.txt").getPath());
        type.createOrReplaceTempView("route_types");
    }

    private static void processGTFSDataBuildSchedules(SQLContext sqlContext) {
        Dataset<Row> schedule = sqlContext
                .sql("SELECT CONCAT(route_types.short_name, ' ', route_short_name) AS route, direction_id, " +
                        "trips.route_id, stop_name, stop_lat, stop_lon, stops.stop_id as stop_id, arrival_time, departure_time, stop_sequence, stop_times.trip_id as trip FROM stop_times " +
                        "INNER JOIN stops ON stop_times.stop_id=stops.stop_id " +
                        "INNER JOIN trips on stop_times.trip_id=trips.trip_id " +
                        "INNER JOIN gtfs_routes on gtfs_routes.route_id=trips.route_id " +
                        "INNER JOIN route_types ON route_types.route_type=gtfs_routes.route_type");
        //direction 0 is Forth, 1 is Back
        schedule = schedule.withColumn("direction",
                when(col("direction_id").equalTo(0), "Forth")
                        .when(col("direction_id").equalTo(1), "Back"));
        schedule = schedule.orderBy(
                col("route"),
                col("direction"),
                col("arrival_time"));
        schedule.createOrReplaceTempView("schedule");
    }

    private static void processEventsData(SQLContext sqlContext) {
        Dataset<Row> transportEvents = sqlContext
                .sql("SELECT VehicleMessageID, SentDate, VehicleID, event_type.Code, " +
                        "WGS84Fi, WGS84La, Odometer, Delay, ShiftID, TripID FROM routes " +
                        "INNER JOIN event_type ON routes.SendingReason=event_type.SendingReason");
        transportEvents = transportEvents
                .filter(transportEvents.col("Code").equalTo("DoorsOpen"));
        //.or(transportEvents.col("Code").equalTo("DoorsClosed")));
        //.or(transportEvents.col("Code").equalTo("Periodical")));
        transportEvents.createOrReplaceTempView("transport_events");
        Dataset<Row> validationEvents = sqlContext
                .sql("SELECT GarNr as GN, TMarsruts, Virziens, ValidTalonaId, Laiks FROM tickets");
        UserDefinedFunction garageNumber = udf(
                (Integer i) -> {
                    while (i > 9999) {
                        i = i / 10;
                    }
                    return i;
                }, DataTypes.IntegerType
        );
        validationEvents = validationEvents.withColumn("GarNr",
                garageNumber.apply(validationEvents.col("GN"))).drop("GN");
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        UserDefinedFunction time = udf(
                (String s) -> new Timestamp(dateFormat.parse(s).getTime()), DataTypes.TimestampType
        );
        validationEvents = validationEvents.withColumn("time",
                time.apply(validationEvents.col("Laiks"))).drop(validationEvents.col("Laiks"));
        validationEvents.createOrReplaceTempView("validation_events");
        Dataset<Row> vehiclesOnRoute = sqlContext
                .sql("SELECT first(TMarsruts) as route, GarNr, vehicles.VehicleID, min(time) as first_time, max(time) as last_time, count(time) as events FROM validation_events " +
                        "INNER JOIN vehicles ON validation_events.GarNr=vehicles.VehicleCompanyCode "
                        + "GROUP BY GarNr, vehicles.VehicleID ORDER BY first_time");
        //In Validations table for 'Tr 101' we have 119 records garage numbers of vehicles,
        //in Vehicles table we have only 93 records with connected VehicleID
        vehiclesOnRoute.createOrReplaceTempView("vehicles_on_route");
        Seq<String> vehicleIDColumn =  new Set.Set1<>("VehicleID").toSeq();
        transportEvents = transportEvents
                .join(vehiclesOnRoute, vehicleIDColumn, "inner");
        transportEvents = transportEvents
                .withColumn("timestamp_of_transport_event", transportEvents.col("SentDate"));
        transportEvents.createOrReplaceTempView("transport_events");
        Seq<String> garageNumberColumn =  new Set.Set1<>("GarNr").toSeq();
        validationEvents = validationEvents
                .join(vehiclesOnRoute, garageNumberColumn, "inner");
        validationEvents.createOrReplaceTempView("validation_events");

        StructType validationSchema = validationEvents.schema();
        List<String> transportFields = Arrays.asList(transportEvents.schema().fieldNames());
        for(StructField e : validationSchema.fields()) {
            if (!transportFields.contains(e.name())) {
                transportEvents = transportEvents
                        .withColumn(e.name(),
                                lit(null));
                transportEvents = transportEvents.withColumn(e.name(),
                        transportEvents.col(e.name()).cast(Optional.ofNullable(e.dataType()).orElse(StringType)));
            }
        }
        StructType transportSchema = transportEvents.schema();
        List<String> validationFields = Arrays.asList(validationEvents.schema().fieldNames());
        for(StructField e : transportSchema.fields()) {
            if (!validationFields.contains(e.name())) {
                validationEvents = validationEvents
                        .withColumn(e.name(),
                                lit(null));
                validationEvents = validationEvents.withColumn(e.name(),
                        validationEvents.col(e.name()).cast(Optional.ofNullable(e.dataType()).orElse(StringType)));
            }
        }
        transportEvents = transportEvents
                .withColumn("event_source",
                        lit("vehicle"));
        validationEvents = validationEvents
                .withColumn("event_source",
                        lit("passenger"));
        transportEvents = transportEvents.unionByName(validationEvents);
        transportEvents = transportEvents
                .withColumn("timestamp", coalesce(col("time"), col("SentDate")))
                .drop("time", "SentDate");
        transportEvents = transportEvents
                .sort(transportEvents.col("GarNr"),
                        transportEvents.col("timestamp"));
        transportEvents.createOrReplaceTempView("transport_events");

        Dataset<Row> consolidated = sqlContext
                .sql("SELECT VehicleID, GarNr, TMarsruts, route, Virziens, Code, WGS84Fi, WGS84La, " +
                        "Odometer, VehicleMessageID, event_source, ValidTalonaId, timestamp, timestamp_of_transport_event " +
                        "FROM transport_events")
                .withColumn("route", coalesce(col("TMarsruts"), col("route")))
                .drop("TMarsruts");
        consolidated.createOrReplaceTempView("transport_events");

        //Fill missed values with Window function after union two different sets
        WindowSpec ws = Window
                .partitionBy(consolidated.col("GarNr"))
                .orderBy(consolidated.col("timestamp"))
                .rowsBetween(Integer.MIN_VALUE + 1, Window.currentRow());
        Column hypotheticalFi = last(consolidated.col("WGS84Fi"), true).over(ws);
        Column hypotheticalLa = last(consolidated.col("WGS84La"), true).over(ws);
        Column timePassed = last(consolidated.col("timestamp_of_transport_event"), true).over(ws);
        Column vehicleMessageId = last(consolidated.col("VehicleMessageID"), true).over(ws);
        consolidated = consolidated
                .withColumn("hypothetical_fi", hypotheticalFi)
                .withColumn("hypothetical_la", hypotheticalLa)
                .withColumn("time_of_last_transport_event", timePassed)
                .withColumn("vehicle_message_id", vehicleMessageId);
        consolidated = consolidated.withColumn("time_between_validation_and_transport_event",
                unix_timestamp(consolidated.col("timestamp"))
                        .minus(unix_timestamp(consolidated.col("time_of_last_transport_event"))))
                .drop("timestamp_of_transport_event");
        consolidated = consolidated.filter(col("event_source").eqNullSafe("passenger"));
        consolidated.createOrReplaceTempView("transport_events");
        consolidated = sqlContext
                .sql("SELECT route, " +
                        "vehicle_message_id, " +
                        "VehicleID, " +
                        "GarNr, " +
                        "hypothetical_fi, " +
                        "hypothetical_la, " +
                        "Virziens, " +
                        "ValidTalonaId, " +
                        "timestamp, " +
                        "time_between_validation_and_transport_event " +
                        "FROM transport_events");
        Dataset<Row> actualStopsAndEvents = consolidated.groupBy(
                col("vehicle_message_id"),
                col("route"),
                col("hypothetical_fi"),
                col("hypothetical_la")
        ).count();
        Dataset<Row> schedule = sqlContext.sql("SELECT route, stop_name, stop_lat, stop_lon FROM schedule");
        Dataset<Row> actualStopsAndRealStops = actualStopsAndEvents
                .join(schedule, new Set.Set1<>("route").toSeq(), "inner");
        //lat degree (56.9) = 111.3 km, lon degree (24) = 60.8 km, coefficient = 60.8 / 111.3
        double coefficient = 0.5462713387241689;
        actualStopsAndRealStops = actualStopsAndRealStops.withColumn("diff",
                abs(actualStopsAndRealStops.col("stop_lat")
                        .$minus(actualStopsAndRealStops.col("hypothetical_fi"))).$times(coefficient).
                        plus(abs(actualStopsAndRealStops.col("stop_lon")
                                .$minus(actualStopsAndRealStops.col("hypothetical_la")))));
        Dataset<Row> minimals = actualStopsAndRealStops.groupBy(
                col("vehicle_message_id"),
                //col("route"),
                col("hypothetical_fi"),
                col("hypothetical_la"))
                .agg(min(col("diff")).as("min"));
        actualStopsAndRealStops = minimals.join(actualStopsAndRealStops, new Set.Set3<>("vehicle_message_id", "hypothetical_fi", "hypothetical_la").toSeq())
                .where(minimals.col("min")
                        .equalTo(actualStopsAndRealStops.col("diff"))).drop("min").orderBy(col("hypothetical_fi"), col("stop_name"));
        //Since the distance is relatively small, we can use the rectangular distance approximation using formula
        //SQRT(POW((stop_lat - hypothetical_fi), 2) + POW((stop_lon - hypothetical_la), 2))
        //but we need to translate grades into km.
        //This approximation is faster than using the Haversine formula.
        //But for comparing distances we can compare squares of coordinate differences without square root calculation.
        actualStopsAndRealStops = actualStopsAndRealStops.withColumn("distance_km",
                sqrt(pow((actualStopsAndRealStops.col("stop_lat")
                        .minus(actualStopsAndRealStops.col("hypothetical_fi")).multiply(111.3)), 2).
                        plus(pow((actualStopsAndRealStops.col("stop_lon")
                                .minus(actualStopsAndRealStops.col("hypothetical_la")).multiply(60.8)), 2))));
        //actualStopsAndRealStops.show();
        actualStopsAndRealStops = actualStopsAndRealStops.drop("hypothetical_fi", "hypothetical_la", "route");
        Seq<String> minimalDistanceColumn =  new Set.Set1<>("vehicle_message_id").toSeq();
        Dataset<Row> result = consolidated.join(actualStopsAndRealStops.as("s"), minimalDistanceColumn, "inner")
                .drop("vehicle_message_id", "diff");
        String dir = UUID.randomUUID().toString();
        result.coalesce(1).write()
                .option("header", "true").csv(System.getProperty("user.dir") + "/result/" + dir);
    }
}
