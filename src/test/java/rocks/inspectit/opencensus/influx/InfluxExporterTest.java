package rocks.inspectit.opencensus.influx;

import com.google.common.collect.ImmutableMap;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import rocks.inspectit.opencensus.influx.utils.PointUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.*;
import static rocks.inspectit.opencensus.influx.utils.OpenCensusUtils.*;

public class InfluxExporterTest {

    private InfluxExporter exporter;

    private InfluxDB influxDB;

    @BeforeEach
    public void beforeTest() {
        influxDB = mock(InfluxDB.class);

        exporter = new InfluxExporter("", "", "", "", "", false);
        exporter.setInflux(influxDB);

        resetOpenCensus();
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
            Measure.MeasureLong measure = createLongMeasure("test_measure");
            createView(measure, "test_measure/count", Aggregation.Count.create(), Collections.emptyList());

            exporter.export();

            ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);
            verify(influxDB).write(pointsCaptor.capture());
            verifyNoMoreInteractions(influxDB);

            assertThat(pointsCaptor.getValue().getPoints()).isEmpty();
        }

        @Test
        public void writeSingleData() {
            Measure measure = createLongMeasure("test_measure");
            createView(measure, "test_measure/count", Aggregation.Count.create(), Collections.emptyList());
            recordData(measure, 100L, Collections.emptyMap());

            exporter.export();

            ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);
            verify(influxDB).write(pointsCaptor.capture());
            verifyNoMoreInteractions(influxDB);

            List<Point> points = pointsCaptor.getValue().getPoints();
            assertThat(points).hasSize(1);

            Point point = points.get(0);
            assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
            assertThat(PointUtils.getField(point)).containsExactly(entry("count", 1L));
            assertThat(PointUtils.getTags(point)).isEmpty();
        }

        @Test
        public void successiveExports() {
            Measure measure = createLongMeasure("test_measure");
            createView(measure, "test_measure/count", Aggregation.Count.create(), Collections.emptyList());
            recordData(measure, 100L, Collections.emptyMap());

            exporter.export();

            recordData(measure, 100L, Collections.emptyMap());

            exporter.export();

            ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);
            verify(influxDB, times(2)).write(pointsCaptor.capture());
            verifyNoMoreInteractions(influxDB);

            List<BatchPoints> values = pointsCaptor.getAllValues();
            assertThat(values).hasSize(2);

            for (int i = 0; i < 2; i++) {
                BatchPoints batchPoints = values.get(i);
                long expected = i + 1L;

                List<Point> points = batchPoints.getPoints();
                assertThat(points).hasSize(1);
                Point point = points.get(0);
                assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
                assertThat(PointUtils.getField(point)).containsExactly(entry("count", expected));
                assertThat(PointUtils.getTags(point)).isEmpty();
            }
        }

        @Test
        public void writeDataWithTags() {
            Measure measure = createLongMeasure("test_measure");
            createView(measure, "test_measure/value", Aggregation.Sum.create(), Arrays.asList("my_tag", "another_tag"));
            recordData(measure, 100L, ImmutableMap.of("my_tag", "my_first_value", "another_tag", "my_second_value"));
            recordData(measure, 200L, Collections.singletonMap("another_tag", "my_second_value"));

            exporter.export();

            ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);
            verify(influxDB).write(pointsCaptor.capture());
            verifyNoMoreInteractions(influxDB);

            List<Point> points = pointsCaptor.getValue().getPoints();
            assertThat(points).hasSize(2);

            assertThat(points).anySatisfy((point) -> {
                assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
                assertThat(PointUtils.getField(point)).containsExactly(entry("value", 100L));
                assertThat(PointUtils.getTags(point)).containsOnly(
                        entry("my_tag", "my_first_value"),
                        entry("another_tag", "my_second_value"));
            });

            assertThat(points).anySatisfy((point) -> {
                assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
                assertThat(PointUtils.getField(point)).containsExactly(entry("value", 200L));
                assertThat(PointUtils.getTags(point)).containsOnly(entry("another_tag", "my_second_value"));
            });
        }

        @Test
        public void writeDistributionData() {
            Aggregation distribution = Aggregation.Distribution.create(BucketBoundaries.create(
                    Arrays.asList(0.0, 500.0, 1000.0)
            ));

            Measure measure = createLongMeasure("test_measure");
            createView(measure, "test_measure/value", distribution, Collections.emptyList());
            recordData(measure, 50L, Collections.emptyMap());
            recordData(measure, 1000L, Collections.emptyMap());
            recordData(measure, 1337L, Collections.emptyMap());

            exporter.export();

            ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);
            verify(influxDB).write(pointsCaptor.capture());
            verifyNoMoreInteractions(influxDB);

            List<Point> points = pointsCaptor.getValue().getPoints();
            assertThat(points).hasSize(4);

            assertThat(points).anySatisfy((point) -> {
                assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
                assertThat(PointUtils.getField(point)).containsOnly(
                        entry("value_count", 3L),
                        entry("value_sum", 2387.0D));
                assertThat(PointUtils.getTags(point)).isEmpty();
            });
            assertThat(points).anySatisfy((point) -> {
                assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
                assertThat(PointUtils.getField(point)).containsExactly(entry("value_bucket", 1L));
                assertThat(PointUtils.getTags(point)).containsExactly(entry("bucket", "(-Inf,500.0]"));
            });
            assertThat(points).anySatisfy((point) -> {
                assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
                assertThat(PointUtils.getField(point)).containsExactly(entry("value_bucket", 0L));
                assertThat(PointUtils.getTags(point)).containsExactly(entry("bucket", "(500.0,1000.0]"));
            });
            assertThat(points).anySatisfy((point) -> {
                assertThat(PointUtils.getMeasurement(point)).isEqualTo("test_measure");
                assertThat(PointUtils.getField(point)).containsExactly(entry("value_bucket", 2L));
                assertThat(PointUtils.getTags(point)).containsExactly(entry("bucket", "(1000.0,+Inf)"));
            });
        }
    }
}
