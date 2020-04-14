package rocks.inspectit.opencensus.influx.utils;

import org.influxdb.dto.Point;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

public class PointUtils {

    public static String getMeasurement(Point point) {
        try {
            Field measurement = Point.class.getDeclaredField("measurement");
            measurement.setAccessible(true);
            return (String) measurement.get(point);
        } catch (Exception e) {
            return null;
        }
    }

    public static Map<String, Object> getField(Point point) {
        try {
            Field fieldsField = Point.class.getDeclaredField("fields");
            fieldsField.setAccessible(true);
            return (Map<String, Object>) fieldsField.get(point);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public static Map<String, String> getTags(Point point) {
        try {
            Field tags = Point.class.getDeclaredField("tags");
            tags.setAccessible(true);
            return (Map<String, String>) tags.get(point);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

}
