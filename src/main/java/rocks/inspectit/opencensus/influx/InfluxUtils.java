package rocks.inspectit.opencensus.influx;

import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.export.MetricDescriptor;
import io.opencensus.stats.View;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InfluxUtils {

    private InfluxUtils() {
    }

    public static String getRawMeasurementName(String metricName, View view) {
        if (view == null) {
            return metricName;
        }
        return view.getMeasure().getName();
    }

    public static String getRawFieldName(MetricDescriptor.Type metricType, String rawMetricName, String rawMeasurementName) {
        String fieldName = removeCommonPrefix(rawMetricName, rawMeasurementName);
        String sanitizedFieldName = sanitizeName(fieldName);
        if (sanitizedFieldName.isEmpty()) {
            return getDefaultFieldName(metricType);
        }
        return fieldName;
    }

    public static Map<String, String> createTagMaps(List<LabelKey> labelKeys, List<LabelValue> labelValues) {
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

    /**
     * @return Returns the timestamp represented by the given point. The timestamp represents the number of milliseconds since the Unix Epoch.
     */
    public static long getTimestampOfPoint(io.opencensus.metrics.export.Point point) {
        Timestamp timestamp = point.getTimestamp();
        return timestamp.getNanos() / 1000 / 1000 + timestamp.getSeconds() * 1000;
    }

    static String sanitizeName(String name) {
        return name.replaceAll("^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "")
                .replaceAll("[^a-zA-Z0-9]+", "_")
                .toLowerCase();
    }

    private static String getDefaultFieldName(MetricDescriptor.Type metricType) {
        switch (metricType) {
            case CUMULATIVE_DOUBLE:
            case CUMULATIVE_INT64:
                return "counter";
            case CUMULATIVE_DISTRIBUTION:
                return "histogram"; // for distributions, "_bucket", "_sum" and "_count" are suffixed
        }
        return "value";
    }

    private static String removeCommonPrefix(String str, String prefixStr) {
        int commonLen = 0;
        int limit = Math.min(str.length(), prefixStr.length());
        while (commonLen < limit && str.charAt(commonLen) == prefixStr.charAt(commonLen)) {
            commonLen++;
        }
        return str.substring(commonLen);
    }
}
