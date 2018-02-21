package de.dentrassi.bcx18.kafka;

import static java.nio.file.Files.list;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.eclipse.iot.unide.ppmp.PPMPPackager;
import org.eclipse.iot.unide.ppmp.measurements.MeasurementsWrapper;

import com.google.common.io.MoreFiles;

public class Converter {

    private static Processor processor = new Processor();

    public static void main(final String[] args) throws IOException {

        list(Paths.get(""))
                .map(Converter::read)
                .map(Converter::toSql);

    }

    private static MeasurementsWrapper read(final Path file) {

        try {
            final String json = MoreFiles.asCharSource(file, StandardCharsets.UTF_8).read();
            return new PPMPPackager().getMeasurementsBean(json);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static String toSql(final MeasurementsWrapper mw) {
        return processor.insert(mw);
    }
}
