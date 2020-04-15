package rocks.inspectit.opencensus.influx;

import lombok.Value;

import java.util.HashMap;
import java.util.Map;

/**
 * This cache is responsible for calculate the change of a metric and keep track of the last values.
 */
public class StatisticsCache {

    /**
     * Map storing the last metric value for a specific metric.
     */
    private Map<CacheKey, Number> valueMap = new HashMap<>();

    /**
     * @return the change of a metric since the last invocation of this method
     */
    public Number getDifference(String measurementName, String fieldName, Map<String, String> tags, Number value) {
        CacheKey cacheKey = createCacheKey(measurementName, fieldName, tags);

        Number latestValue = valueMap.put(cacheKey, value);

        if (latestValue == null) {
            latestValue = 0L;
        }

        Number difference;
        if (value instanceof Long) {
            difference = value.longValue() - latestValue.longValue();
        } else {
            difference = value.doubleValue() - latestValue.doubleValue();
        }

        return difference;
    }

    /**
     * @return the key which is used for caching the last metric value
     */
    private CacheKey createCacheKey(String measurementName, String fieldName, Map<String, String> tags) {
        return new CacheKey(measurementName, fieldName, new HashMap<>(tags));
    }

    @Value
    private static class CacheKey {
        private String measurementName;

        private String fieldName;

        private Map<String, String> tag;
    }
}
