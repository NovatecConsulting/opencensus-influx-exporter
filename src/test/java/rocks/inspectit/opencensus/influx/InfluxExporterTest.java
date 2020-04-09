package rocks.inspectit.opencensus.influx;

import io.opencensus.common.Timestamp;
import io.opencensus.implcore.stats.ViewManagerImpl;
import io.opencensus.metrics.export.*;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Measure;
import io.opencensus.stats.Stats;
import io.opencensus.stats.View;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tags;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import rocks.inspectit.opencensus.influx.utils.OpenCensusDummyData;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class InfluxExporterTest {

    private InfluxExporter exporter;

    private InfluxDB influxDB;

    @BeforeAll
    public static void before() {
        TagKey KEY_ERROR = TagKey.create("myTag");
        TagKey KEY_ERROR2 = TagKey.create("anotherTag");
        Measure.MeasureLong M_LINE_LENGTHS = Measure.MeasureLong.create("testmetric/lines_in", "The distribution of line lengths", "By");

        View view = View.create(View.Name.create("testmetric/lines_in/count"), "empty", M_LINE_LENGTHS, Aggregation.Count.create(), Arrays.asList(KEY_ERROR, KEY_ERROR2));

        Stats.getViewManager().registerView(view);

        TagContext tctx = Tags.getTagger().emptyBuilder().putLocal(KEY_ERROR, TagValue.create("marius")).build();
        Stats.getStatsRecorder().newMeasureMap().put(M_LINE_LENGTHS, 1337L).record(tctx);

        TagContext tctx2 = Tags.getTagger().emptyBuilder().putLocal(KEY_ERROR, TagValue.create("test123")).putLocal(KEY_ERROR2, TagValue.create("xxx")).build();
        Stats.getStatsRecorder().newMeasureMap().put(M_LINE_LENGTHS, 1337L).record(tctx2);
    }

    @BeforeEach
    public void beforeTest() {
        influxDB = mock(InfluxDB.class);

        exporter = new InfluxExporter("", "", "", "", "", false);
        exporter.setInflux(influxDB);
    }

    public void injectMetrics(Metric metric) {
        exporter.setMetricProducerSupplier(() -> Collections.singleton(new MetricProducer() {
            @Override
            public Collection<Metric> getMetrics() {
                return Collections.singleton(metric);
            }
        }));
    }

    public void injectMetrics(Collection<Metric> metrics) {
        exporter.setMetricProducerSupplier(() -> Collections.singleton(new MetricProducer() {
            @Override
            public Collection<Metric> getMetrics() {
                return metrics;
            }
        }));
    }

    public void injectView(View... views) {
        Set<View> viewSet = new HashSet<>(Arrays.asList(views));
        injectView(viewSet);
    }

    public void injectData(OpenCensusDummyData data) {
        Collection<Metric> metrics = data.getMetrics();
        exporter.setMetricProducerSupplier(() -> Collections.singleton(new MetricProducer() {
            @Override
            public Collection<Metric> getMetrics() {
                return new ArrayList<>(metrics);
            }
        }));
    }

    public void injectView( Set<View> views ) {
        exporter.setViewSupplier(() -> views);
    }

    public Metric createMetric(String name, MetricDescriptor.Type type) {
        return createMetric(name, type, null);
    }

    public Metric createMetric(String name, MetricDescriptor.Type type, Long number) {
        MetricDescriptor metricDescriptor = MetricDescriptor.create(name, "Description", "unit", type, Collections.emptyList());
        TimeSeries timeSeries = TimeSeries.create(Collections.emptyList());

        if (number != null) {
            Value value = Value.longValue(1L);
            Timestamp timestamp = Timestamp.fromMillis(1586356695608L);
            Point point = Point.create(value, timestamp);

            timeSeries = timeSeries.setPoint(point);
        }

        return Metric.createWithOneTimeSeries(metricDescriptor, timeSeries);
    }

    @Nested
    class Export {

        @Test
        public void noMetrics() {

            exporter.export();

            verifyNoInteractions(influxDB);
        }

        @Test
        public void noData() {
            Metric dummyMetric = createMetric("dummyMetric", MetricDescriptor.Type.CUMULATIVE_INT64);
            injectMetrics(dummyMetric);

            exporter.export();

            ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);
            verify(influxDB).write(pointsCaptor.capture());
            verifyNoMoreInteractions(influxDB);

            assertThat(pointsCaptor.getValue().getPoints()).isEmpty();
        }

        @Test
        public void writeData() throws Exception {
            OpenCensusDummyData dummyData = new OpenCensusDummyData();
            OpenCensusDummyData.DummyView dummyView = dummyData.addLongView("dummyMetric", "dummyMetric/count", Aggregation.Count.create(), MetricDescriptor.Type.CUMULATIVE_INT64, Collections.singletonList("testTag"));
            dummyView.setLongData(Collections.singletonList("testValue"), 1L);
            dummyView.setLongData(Collections.singletonList("testValue2"), 10L);

            //injectMetrics(dummyData.getMetrics());
            //injectView(dummyData.getViews());

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

            exporter.export();

            ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);
            verify(influxDB).write(pointsCaptor.capture());
            verifyNoMoreInteractions(influxDB);

            assertThat(pointsCaptor.getValue().getPoints()).isEmpty();
        }
    }
}
