package rocks.inspectit.opencensus.influx;


import io.opencensus.metrics.Metrics;
import io.opencensus.metrics.export.Distribution;
import io.opencensus.metrics.export.Metric;
import io.opencensus.metrics.export.MetricProducer;
import io.opencensus.metrics.export.Value;
import io.opencensus.stats.Stats;
import io.opencensus.stats.View;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.msgpack.core.annotations.VisibleForTesting;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class InfluxExporter implements AutoCloseable {

    private final String url;

    private final String user;

    private final String password;

    private final String database;

    private final String retention;

    private boolean createDatabase;

    @Setter(AccessLevel.PACKAGE)
    private InfluxDB influx;

    @Setter(AccessLevel.PACKAGE)
    private Supplier<Set<MetricProducer>> metricProducerSupplier;

    @Setter(AccessLevel.PACKAGE)
    private Supplier<Set<View>> viewSupplier;

    /**
     * Creates a new influx Exporter.
     * The export does not export by itself, but instead has to be triggered manually by calling {@link #export()};
     * This can be done periodically for example using {@link java.util.concurrent.ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
     *
     * @param url            The http url of the influx to connect to
     * @param user           the user to use when connecting to influx, can be null
     * @param password       the password to use when connecting to influx, can be null
     * @param database       the influx database to
     * @param retention      the retention policy of the database to write to
     * @param createDatabase if true, the exporter will create the specified database with an "autogen" policy when it connects
     */
    @Builder
    public InfluxExporter(String url, String user, String password, String database, String retention, boolean createDatabase) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;
        this.retention = retention;
        this.createDatabase = createDatabase;

        metricProducerSupplier = () -> Metrics.getExportComponent().getMetricProducerManager().getAllMetricProducer();
        viewSupplier = () -> Stats.getViewManager().getAllExportedViews();
    }

    /**
     * Fetches all metrics from OpenCensus and attempts to export them to Influx.
     * No exception is thrown in case of a failure, instead the data is jsut not written and the error is logged.
     */
    public synchronized void export() {
        List<Metric> metrics = metricProducerSupplier.get()
                .stream()
                .flatMap(metricProducer -> metricProducer.getMetrics().stream())
                .collect(Collectors.toList());

        export(metrics);
    }

    private synchronized void export(Collection<Metric> metrics) {
        if (metrics.size() <= 0) {
            return;
        }

        Map<String, View> viewsMap = viewSupplier.get()
                .stream()
                .collect(Collectors.toMap(view -> view.getName().asString(), view -> view));

        connectAndCreateDatabase();

        if (influx != null) {
            try {
                BatchPoints batch = BatchPoints.database(database).retentionPolicy(retention).build();
                metrics.stream()
                        .flatMap(metric -> {
                            String metricName = metric.getMetricDescriptor().getName();
                            View view = viewsMap.get(metricName);

                            String measurementName = InfluxUtils.getMeasurementName(metricName, view);
                            String fieldName = InfluxUtils.getFieldName(metric.getMetricDescriptor().getType(), view);

                            return toInfluxPoints(metric, measurementName, fieldName);
                        })
                        .filter(Objects::nonNull)
                        .forEach(batch::point);

                influx.write(batch);
            } catch (Exception e) {
                log.error("Error writing to InfluxDB", e);
            }
        }
    }

    private Stream<Point> toInfluxPoints(Metric metric, String measurementName, String fieldName) {
        return metric.getTimeSeriesList().stream()
                .flatMap(timeSeries -> {
                    Map<String, String> tags = InfluxUtils.createTagMaps(metric.getMetricDescriptor().getLabelKeys(), timeSeries.getLabelValues());

                    return timeSeries.getPoints()
                            .stream()
                            .flatMap(point -> toInfluxPoint(point, measurementName, fieldName, tags));
                });
    }

    private Stream<Point> toInfluxPoint(io.opencensus.metrics.export.Point point, String measurementName, String fieldName, Map<String, String> tags) {
        long pointTime = InfluxUtils.getTimestampOfPoint(point);

        Value pointValue = point.getValue();

        Stream<Point.Builder> builderStream = pointValue.match(
                doubleValue -> Stream.of(Point.measurement(measurementName).addField(fieldName, doubleValue)),
                longValue -> Stream.of(Point.measurement(measurementName).addField(fieldName, longValue)),
                distributionValue -> getDistributionPoints(distributionValue, measurementName, fieldName),
                null,
                null);

        return builderStream
                .map(builder -> builder.time(pointTime, TimeUnit.MILLISECONDS))
                .map(builder -> builder.tag(tags))
                .map(Point.Builder::build);
    }

    private Stream<Point.Builder> getDistributionPoints(Distribution distr, String measurementName, String fieldName) {
        String prefix = fieldName.isEmpty() ? "" : fieldName + "_";
        List<Point.Builder> results = new ArrayList<>();
        results.add(
                Point.measurement(measurementName)
                        .addField(prefix + "sum", distr.getSum())
                        .addField(prefix + "count", distr.getCount())
        );
        List<Double> bucketBoundaries =
                distr.getBucketOptions().match(Distribution.BucketOptions.ExplicitOptions::getBucketBoundaries, (noOp) -> Collections.emptyList());
        for (int i = 0; i < distr.getBuckets().size(); i++) {
            Distribution.Bucket bucket = distr.getBuckets().get(i);
            String lowerLimit = i > 0 ? "(" + bucketBoundaries.get(i - 1) : "(-Inf";
            String upperLimit = i < bucketBoundaries.size() ? bucketBoundaries.get(i) + "]" : "+Inf)";
            String interval = lowerLimit + "," + upperLimit;
            results.add(
                    Point.measurement(measurementName)
                            .addField(prefix + "bucket", bucket.getCount())
                            .tag("bucket", interval)
            );
        }
        return results.stream();
    }

    private synchronized void connectAndCreateDatabase() {
        try {
            if (influx == null) {
                if (user == null || password == null) {
                    influx = InfluxDBFactory.connect(url);
                } else {
                    influx = InfluxDBFactory.connect(url, user, password);
                }
                if (createDatabase) {
                    QueryResult query = influx.query(new Query("CREATE DATABASE " + database));
                    String error = query.getError();
                    if (error != null) {
                        log.error("Error creating database: {}", error);
                    }
                }
            }
        } catch (Throwable t) {
            influx = null;
            log.error("Could not connect to influx", t);
        }
    }

    /**
     * Closes the underlying InfluxDB connection.
     * If {@link #export()} is invoked after {@link #close()}, the connection will be opened again.
     */
    @Override
    public synchronized void close() {
        if (influx != null) {
            influx.close();
            influx = null;
        }
    }
}
