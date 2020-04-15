package rocks.inspectit.opencensus.influx;

import java.util.HashMap;
import java.util.Map;

/**
 * This cache is responsible for calculate the change of a metric and keep track of the last values.
 */
public class StatisticsCache {

    /**
     * Map storing the last metric value for a specific metric.
     */
    private Map<String, Number> valueMap = new HashMap<>();

    /**
     * @return the change of a metric since the last invocation of this method
     */
    public Number getDifference(String measurementName, String fieldName, Map<String, String> tags, Number value) {
        String cacheKey = cacheKey(measurementName, fieldName, tags);

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
     * This method generates a key used for caching the values.
     * The following shows example keys:
     * <p><ul>
     * <li>test_measure/value_count/{}
     * <li>test_measure/value/{another_tag=my_second_value}
     * </ul><p>
     *
     * @return the key which is used for caching the last metric value
     */
    private String cacheKey(String measurementName, String fieldName, Map<String, String> tag) {
        return measurementName + "/" + fieldName + "/" + tag.toString();
    }
}
