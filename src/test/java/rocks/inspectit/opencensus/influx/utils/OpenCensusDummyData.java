package rocks.inspectit.opencensus.influx.utils;

import io.opencensus.common.Timestamp;
import io.opencensus.metrics.LabelKey;
import io.opencensus.metrics.LabelValue;
import io.opencensus.metrics.export.*;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Measure;
import io.opencensus.stats.View;
import io.opencensus.tags.TagKey;

import java.util.*;
import java.util.stream.Collectors;

public class OpenCensusDummyData {

    private Set<View> views = new HashSet<>();

    private Collection<DummyView> dummyViews = new HashSet<>();

    public DummyView addLongView(String measureName, String viewName, Aggregation aggregation, MetricDescriptor.Type type, List<String> tags) {
        return addView(measureName, viewName, aggregation, type, tags, true);
    }

    public Set<View> getViews() {
        return views;
    }

    private DummyView addView(String measureName, String viewName, Aggregation aggregation, MetricDescriptor.Type type, List<String> tags, boolean longView) {
        List<TagKey> tagKeys = tags.stream()
                .map(TagKey::create)
                .collect(Collectors.toList());

        Measure measure;
        if (longView) {
            measure = Measure.MeasureLong.create(measureName, "description", "unit");
        } else {
            measure = Measure.MeasureDouble.create(measureName, "description", "unit");
        }
        View.Name viewNameObject = View.Name.create(viewName);
        View view = View.create(viewNameObject, "", measure, aggregation, tagKeys);

        views.add(view);

        DummyView dummyView = new DummyView(viewName, tags, type);
        dummyViews.add(dummyView);
        return dummyView;
    }

    public Collection<Metric> getMetrics() {
        return dummyViews.stream()
                .map(view -> Metric.create(view.metricDescriptor, new ArrayList<>(view.timeSeriesMap.values())))
                .collect(Collectors.toSet());
    }

    public static class DummyView {

        private MetricDescriptor metricDescriptor;

        private Map<List<LabelValue>, TimeSeries> timeSeriesMap = new HashMap<List<LabelValue>, TimeSeries>();

        public DummyView(String viewName, List<String> tags, MetricDescriptor.Type type) {
            List<LabelKey> labelKeys = tags.stream()
                    .map(tag -> LabelKey.create(tag, "description"))
                    .collect(Collectors.toList());

            metricDescriptor = MetricDescriptor.create(viewName, "description", "unit", type, labelKeys);
        }

        public void setLongData(List<String> tagValues, Long value) {
            List<LabelValue> labelValues = tagValues.stream()
                    .map(LabelValue::create)
                    .collect(Collectors.toList());

            TimeSeries timeSeries = TimeSeries.create(labelValues);

            if (value != null) {
                Value longValue = Value.longValue(1L);
                Timestamp timestamp = Timestamp.fromMillis(1586356695608L);
                Point point = Point.create(longValue, timestamp);

                timeSeries = timeSeries.setPoint(point);
            }

            timeSeriesMap.put(labelValues, timeSeries);
        }
    }
}
