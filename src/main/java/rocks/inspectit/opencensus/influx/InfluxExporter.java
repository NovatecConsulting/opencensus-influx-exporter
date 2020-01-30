package rocks.inspectit.opencensus.influx;


import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.Metrics;
import io.opencensus.metrics.export.Distribution;
import io.opencensus.metrics.export.Metric;
import io.opencensus.metrics.export.MetricDescriptor;
import io.opencensus.stats.Stats;
import io.opencensus.stats.View;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class InfluxExporter implements AutoCloseable {

    private final String url;
    private final String user;
    private final String password;

    private final String database;
    private final String retention;

    private InfluxDB influx;

    private boolean createDatabase;

    /**
     * Creates a new influx Exporter.
     * The export does not export by itself, but instead has to be triggered manually by calling {@link #export()};
     * This can be done periodically for example using {@link java.util.concurrent.ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)}.
     *
     * @param url The http url of the influx to connect to
     * @param user the user to use when connecting to influx, can be null
     * @param password the password to use when connecting to influx, can be null
     * @param database the influx database to
     * @param retention the retention policy of the database to write to
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
    }

    /**
     * Fetches all metrics from OpenCensus and attempts to export them to Influx.
     * No exception is thrown in case of a failure, instead the data is jsut not written and the error is logged.
     */
    public synchronized void export() {
        List<Metric> metrics = Metrics.getExportComponent().getMetricProducerManager().getAllMetricProducer()
                .stream()
                .flatMap(mp -> mp.getMetrics().stream())
                .collect(Collectors.toList());
        export(metrics);
    }

    synchronized void export(Collection<Metric> metrics) {
        Map<String, View> viewsMap = Stats.getViewManager().getAllExportedViews()
                .stream()
                .collect(Collectors.toMap(v -> v.getName().asString(), v -> v));
        connectAndCreateDatabase();
        if (influx != null) {
            try {
                BatchPoints batch = BatchPoints.database(database).retentionPolicy(retention).build();
                metrics.stream()
                        .flatMap(m -> {
                            String metricName = m.getMetricDescriptor().getName();
                            View view = viewsMap.get(metricName);
                            return toInfluxPoints(m, getMeasurementName(metricName, view), getFieldName(m.getMetricDescriptor().getType(), view));
                        })
                        .forEach(batch::point);
                influx.write(batch);
            } catch (Exception e) {
                log.error("Error writing to InfluxDB", e);
            }
        }
    }

    String getMeasurementName(String metricName, View view) {
        if (view == null) {
            return sanitizeName(metricName);
        }
        String measureName = view.getMeasure().getName();
        return sanitizeName(measureName);
    }

    String getFieldName(MetricDescriptor.Type metricType, View view) {
        if (view == null) {
            return getDefaultFieldName(metricType);
        }
        String measureName = view.getMeasure().getName();
        String viewName = view.getName().asString();
        String fieldName = sanitizeName(removeCommonPrefix(viewName, measureName));
        if (fieldName.isEmpty()) {
            return getDefaultFieldName(metricType);
        }
        return fieldName;
    }

    private String getDefaultFieldName(MetricDescriptor.Type metricType) {
        switch (metricType) {
            case CUMULATIVE_DOUBLE:
            case CUMULATIVE_INT64:
                return "counter";
            case CUMULATIVE_DISTRIBUTION:
                return "histogram"; // for distributions, "_bucket", "_sum" and "_count" are suffixed
        }
        return "value";
    }

    private String removeCommonPrefix(String str, String prefixStr) {
        int commonLen = 0;
        int limit = Math.min(str.length(), prefixStr.length());
        while (commonLen < limit && str.charAt(commonLen) == prefixStr.charAt(commonLen)) {
            commonLen++;
        }
        return str.substring(commonLen);
    }


    private String sanitizeName(String name) {
        return name.replaceAll("^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "")
                .replaceAll("[^a-zA-Z0-9]+", "_")
                .toLowerCase();
    }

    private Stream<Point> toInfluxPoints(Metric m, String measurementName, String fieldName) {
        return m.getTimeSeriesList().stream()
                .flatMap(timeSeries -> {
                    Map<String, String> tags = getTags(m.getMetricDescriptor().getLabelKeys(), timeSeries.getLabelValues());
                    return timeSeries.getPoints()
                            .stream()
                            .flatMap(pt -> pt.getValue().match(
                                    doubleVal -> Stream.of(Point.measurement(measurementName).addField(fieldName, doubleVal)),
                                    longVal -> Stream.of(Point.measurement(measurementName).addField(fieldName, longVal)),
                                    distribution -> getDistributionPoints(distribution, measurementName, fieldName),
                                    null, null)
                                    .map(ptb -> ptb.time(getPointMillis(pt), TimeUnit.MILLISECONDS))
                                    .map(ptb -> ptb.tag(tags))
                                    .map(Point.Builder::build)
                            );
                });
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

    Map<String, String> getTags(List<LabelKey> labelKeys, List<LabelValue> labelValues) {
        Iterator<LabelKey> keys = labelKeys.iterator();
        Iterator<LabelValue> values = labelValues.iterator();
        Map<String, String> result = new HashMap<>();
        while (keys.hasNext() && values.hasNext()) {
            String value = values.next().getValue();
            String key = keys.next().getKey();
            if (value != null && key != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    long getPointMillis(io.opencensus.metrics.export.Point pt) {
        Timestamp timestamp = pt.getTimestamp();
        return timestamp.getNanos() / 1000 / 1000 + timestamp.getSeconds() * 1000;
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
     * Closes the underyling influx connection.
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
