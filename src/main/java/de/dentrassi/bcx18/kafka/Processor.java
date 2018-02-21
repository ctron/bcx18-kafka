package de.dentrassi.bcx18.kafka;

import static java.time.format.DateTimeFormatter.ofPattern;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.iot.unide.ppmp.measurements.Measurements;
import org.eclipse.iot.unide.ppmp.measurements.MeasurementsWrapper;
import org.springframework.stereotype.Component;

@Component
public class Processor {

    private static final DateTimeFormatter FORMAT = ofPattern("YYYY-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC);

    public String insert(final MeasurementsWrapper mw) {

        final List<String> fields = new ArrayList<>();
        final List<String> values = new ArrayList<>();

        boolean needTs = true;
        for (final Measurements m : mw.getMeasurements()) {

            for (final Map.Entry<String, List<Number>> entry : m.getSeriesMap().getSeries().entrySet()) {

                final String key = entry.getKey();
                if (key.equals("$_time")) {
                    continue;
                }

                fields.add("`" + entry.getKey() + "`");
                values.add("" + entry.getValue().get(0));

            }

            if (needTs) {
                needTs = false;
                fields.add("TS");
                values.add("\"" + FORMAT.format(m.getTimestamp()) + "\"");
            }
        }

        return "INSERT INTO pack_simulation (" +
                String.join(", ", fields) +
                ") VALUES (" +
                String.join(", ", values)
                + ")";
    }
}
