package rocks.inspectit.opencensus.influx;

import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.export.MetricDescriptor;
import io.opencensus.stats.Measure;
import io.opencensus.stats.View;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class InfluxUtilsTest {

    @Nested
    class GetRawMeasurementName {

        @Test
        void verifyNameSanitizationWithoutView() {
            String result = InfluxUtils.getRawMeasurementName("$%    I am$SPEcial$", null);

            assertThat(result).isEqualTo("$%    I am$SPEcial$");
        }

        @Test
        void verifyMeasureNameUsed() {
            View view = Mockito.mock(View.class);
            Measure measure = Mockito.mock(Measure.class);
            when(view.getMeasure()).thenReturn(measure);
            when(measure.getName()).thenReturn("$%    I am$SPEcial$");

            String result = InfluxUtils.getRawMeasurementName("wrong", view);

            assertThat(result).isEqualTo("$%    I am$SPEcial$");
        }
    }

    @Nested
    class SanitizeName {

        @Test
        void checkSanitization() {
            String result = InfluxUtils.sanitizeName( "$%    I am$SPEcial$");
            assertThat(result).isEqualTo("i_am_special");
        }

    }

    @Nested
    class GetRawFieldName {

        @Test
        void cumulativeDoubleIsCounter() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.CUMULATIVE_DOUBLE, "","");

            assertThat(result).isEqualTo("counter");
        }

        @Test
        void cumulativeLongIsCounter() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.CUMULATIVE_INT64, "","");

            assertThat(result).isEqualTo("counter");
        }

        @Test
        void gaugeDoubleIsValue() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.GAUGE_DOUBLE, "","");

            assertThat(result).isEqualTo("value");
        }

        @Test
        void gaugeLongIsValue() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.GAUGE_INT64, "","");

            assertThat(result).isEqualTo("value");
        }

        @Test
        void distributionIsHistogram() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.CUMULATIVE_DISTRIBUTION, "","");

            assertThat(result).isEqualTo("histogram");
        }


        @Test
        void verifyViewSuffixUsed() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.CUMULATIVE_DOUBLE, "$my$metric$$Data//Point","$my$metric$$");

            assertThat(result).isEqualTo("Data//Point");
        }

        @Test
        void invalidCharacters() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.CUMULATIVE_DOUBLE, "te$st/val","te$st");

            assertThat(result).isEqualTo("/val");
        }

        @Test
        void checkEmptySuffix() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.CUMULATIVE_DOUBLE, "$my$metric$$","$my$metric");

            assertThat(result).isEqualTo("counter");
        }

        @Test
        void defaultNameUsedIfViewAndMeasureEqual() {
            String result = InfluxUtils.getRawFieldName(MetricDescriptor.Type.CUMULATIVE_DOUBLE, "something", "something");

            assertThat(result).isEqualTo("counter");
        }
    }

    @Nested
    class GetTags {

        @Test
        void nullValuesRemoved() {
            List<LabelKey> keys = Arrays.asList(LabelKey.create("a", "a"), LabelKey.create("b", "b"), LabelKey.create("b", "b"));
            List<LabelValue> values = Arrays.asList(LabelValue.create("a_val"), LabelValue.create(null), LabelValue.create("b_val"));

            Map<String, String> result = InfluxUtils.createTagMaps(keys, values);
            assertThat(result).hasSize(2)
                    .containsEntry("a", "a_val")
                    .containsEntry("b", "b_val");
        }
    }


    @Nested
    class GetPointMillis {

        @Test
        void timesAddedCorrectly() {
            Timestamp ts = Timestamp.create(42,99009000);
            io.opencensus.metrics.export.Point pt = Mockito.mock(io.opencensus.metrics.export.Point.class);
            when(pt.getTimestamp()).thenReturn(ts);

            long result = InfluxUtils.getTimestampOfPoint(pt);

            assertThat(result).isEqualTo(42099);
        }
    }
}