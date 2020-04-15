package rocks.inspectit.opencensus.influx;


import io.opencensus.metrics.Metrics;
import io.opencensus.metrics.export.*;
import io.opencensus.metrics.export.Distribution.BucketOptions.ExplicitOptions;
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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class InfluxExporter implements AutoCloseable {

    private static final Number LONG_ZERO = 0L;

    private static final Number DOUBLE_ZERO = 0D;

    private final String url;

    private final String user;

    private final String password;

    private final String database;

    private final String retention;

    private boolean createDatabase;

    private boolean exportDifference;

    @Setter(AccessLevel.PACKAGE)
    private InfluxDB influx;

    private StatisticsCache statsCache;

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
    public InfluxExporter(String url, String user, String password, String database, String retention, boolean createDatabase, boolean exportDifference) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;
        this.retention = retention;
        this.createDatabase = createDatabase;
        this.exportDifference = exportDifference;

        metricProducerSupplier = () -> Metrics.getExportComponent().getMetricProducerManager().getAllMetricProducer();
        viewSupplier = () -> Stats.getViewManager().getAllExportedViews();

        if (exportDifference) {
            statsCache = new StatisticsCache();
            export(true);
        }
    }

    /**
     * Fetches all metrics from OpenCensus and attempts to export them to Influx.
     * No exception is thrown in case of a failure, instead the data is just not written and the error is logged.
     */
    public synchronized void export() {
        export(false);
    }

    /**
     * See documentation of {@link #export()}.
     *
     * @param dryRun If this flag is set to true, the data is not written to the InfluxDB. This is mainly used to initialize
     *               the statistics cache.
     */
    private synchronized void export(boolean dryRun) {
        List<Metric> metrics = metricProducerSupplier.get()
                .stream()
                .flatMap(metricProducer -> metricProducer.getMetrics().stream())
                .collect(Collectors.toList());

        export(metrics, dryRun);
    }

    private synchronized void export(Collection<Metric> metrics, boolean dryRun) {
        if (metrics.size() <= 0) {
            return;
        }

        Map<String, View> viewsMap = viewSupplier.get()
                .stream()
                .collect(Collectors.toMap(view -> view.getName().asString(), view -> view));

        if (!dryRun) {
            connectAndCreateDatabase();
        }

        if (influx != null || dryRun) {
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

                if (!dryRun) {
                    influx.write(batch);
                }
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
                            .flatMap(point -> toInfluxPoint(point, metric, measurementName, fieldName, tags));
                });
    }

    private Stream<Point> toInfluxPoint(io.opencensus.metrics.export.Point point, Metric metric, String measurementName, String fieldName, Map<String, String> tags) {
        long pointTime = InfluxUtils.getTimestampOfPoint(point);

        Value pointValue = point.getValue();

        Stream<Point.Builder> builderStream = pointValue.match(
                doubleValue -> transformValue(metric, measurementName, fieldName, tags, doubleValue),
                longValue -> transformValue(metric, measurementName, fieldName, tags, longValue),
                distributionValue -> getDistributionPoints(distributionValue, measurementName, fieldName, tags),
                null,
                null);

        return builderStream
                .map(builder -> builder.time(pointTime, TimeUnit.MILLISECONDS))
                .map(Point.Builder::build);
    }

    private Stream<Point.Builder> transformValue(Metric metric, String measurementName, String fieldName, Map<String, String> tags, Number value) {
        MetricDescriptor.Type type = metric.getMetricDescriptor().getType();
        boolean isGauge = type == MetricDescriptor.Type.GAUGE_DOUBLE || type == MetricDescriptor.Type.GAUGE_INT64 || type == MetricDescriptor.Type.GAUGE_DISTRIBUTION;

        Optional<Point.Builder> pointBuilder = createPointBuilder(measurementName, fieldName, tags, value, isGauge);
        return pointBuilder.map(Stream::of).orElseGet(Stream::empty);
    }

    private Optional<Point.Builder> createPointBuilder(String measurementName, String fieldName, Map<String, String> tags, Number value) {
        return createPointBuilder(measurementName, fieldName, tags, value, false);
    }

    private Optional<Point.Builder> createPointBuilder(String measurementName, String fieldName, Map<String, String> tags, Number value, boolean isGauge) {
        Optional<Number> processedValue = processValue(isGauge, measurementName, fieldName, tags, value);
        return processedValue.map(number -> Point.measurement(measurementName).addField(fieldName, number).tag(tags));
    }

    private Stream<Point.Builder> getDistributionPoints(Distribution distribution, String measurementName, String fieldName, Map<String, String> tags) {
        String prefix = fieldName.isEmpty() ? "" : fieldName + "_";
        List<Optional<Point.Builder>> results = new ArrayList<>();

        String countFieldName = prefix + "count";
        String sumFieldName = prefix + "sum";

        results.add(createPointBuilder(measurementName, countFieldName, tags, distribution.getCount()));
        results.add(createPointBuilder(measurementName, sumFieldName, tags, distribution.getSum()));

        // temporary tag map used for cache key generation to prevent multiple map creations
        HashMap<String, String> tempTagMap = new HashMap<>(tags);

        List<Double> bucketBoundaries = distribution.getBucketOptions().match(ExplicitOptions::getBucketBoundaries, (noOp) -> Collections.emptyList());
        for (int i = 0; i < distribution.getBuckets().size(); i++) {
            Distribution.Bucket bucket = distribution.getBuckets().get(i);

            String lowerLimit = i > 0 ? "(" + bucketBoundaries.get(i - 1) : "(-Inf";
            String upperLimit = i < bucketBoundaries.size() ? bucketBoundaries.get(i) + "]" : "+Inf)";
            String interval = lowerLimit + "," + upperLimit;

            tempTagMap.put("bucket", interval);

            results.add(createPointBuilder(measurementName, prefix + "bucket", tempTagMap, bucket.getCount()));
        }

        return results.stream()
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    /**
     * Processes the value for the specified field/measure. In case the exporter is exporting the metric difference this
     * method will return the difference of the value compared to the last value. If the metric has not been changed, the
     * result will be empty.
     * In case the exporter is not exporting the difference but the actual value, this method will always return the
     * given value.
     *
     * @return the value to export for the specified metric
     */
    private Optional<Number> processValue(boolean isGauge, String measurementName, String fieldName, Map<String, String> tags, Number value) {
        if (exportDifference) {
            Number difference = statsCache.getDifference(measurementName, fieldName, tags, value);

            if (difference.doubleValue() != 0D) {
                if (isGauge) {
                    return Optional.of(value);
                } else {
                    return Optional.of(difference);
                }
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.of(value);
        }
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
