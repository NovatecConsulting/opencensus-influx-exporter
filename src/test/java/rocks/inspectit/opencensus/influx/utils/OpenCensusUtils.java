package rocks.inspectit.opencensus.influx.utils;

import io.opencensus.impl.internal.DisruptorEventQueue;
import io.opencensus.implcore.stats.ViewManagerImpl;
import io.opencensus.stats.*;
import io.opencensus.tags.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OpenCensusUtils {

    private OpenCensusUtils() {
    }

    public static Measure.MeasureLong createLongMeasure(String name) {
        return Measure.MeasureLong.create(name, "description", "unit");
    }

    public static Measure.MeasureDouble createDoubleMeasure(String name) {
        return Measure.MeasureDouble.create(name, "description", "unit");
    }

    public static void createView(Measure measure, String name, Aggregation aggregation, Collection<String> tags) {
        List<TagKey> tagKeys = tags.stream()
                .map(TagKey::create)
                .collect(Collectors.toList());

        View view = View.create(View.Name.create(name), "description", measure, aggregation, tagKeys);
        Stats.getViewManager().registerView(view);
    }

    public static void recordData(Measure measure, Number value, Map<String, String> tagMap) {
        TagContextBuilder contextBuilder = Tags.getTagger().emptyBuilder();

        tagMap.forEach((key, val) -> {
            TagKey tagKey = TagKey.create(key);
            TagValue tagValue = TagValue.create(val);
            contextBuilder.putLocal(tagKey, tagValue);
        });

        TagContext tagContext = contextBuilder.build();
        MeasureMap measureMap = Stats.getStatsRecorder().newMeasureMap();

        if (measure instanceof Measure.MeasureLong) {
            measureMap.put((Measure.MeasureLong) measure, (long) value);
        } else {
            measureMap.put((Measure.MeasureDouble) measure, (double) value);
        }

        measureMap.record(tagContext);

        waitForDisruptor();
    }

    private static void waitForDisruptor() {
        CountDownLatch latch = new CountDownLatch(1);
        DisruptorEventQueue.getInstance().enqueue(latch::countDown);
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void resetOpenCensus() {
        try {
            Constructor<?>[] constructors = Class.forName("io.opencensus.implcore.stats.MeasureToViewMap").getDeclaredConstructors();
            Constructor<?> constructor = constructors[0];
            constructor.setAccessible(true);
            Object measuretoViewMap = constructor.newInstance();

            Field statsManagerField = ViewManagerImpl.class.getDeclaredField("statsManager");
            statsManagerField.setAccessible(true);
            Object statsManager = statsManagerField.get(Stats.getViewManager());

            Field measureToViewMapField = statsManager.getClass().getDeclaredField("measureToViewMap");
            measureToViewMapField.setAccessible(true);
            measureToViewMapField.set(statsManager, measuretoViewMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed resetting OpenCensus", e);
        }
    }

}
