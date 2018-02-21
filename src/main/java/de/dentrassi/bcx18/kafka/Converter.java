package de.dentrassi.bcx18.kafka;

import static java.nio.file.Files.list;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.eclipse.iot.unide.ppmp.PPMPPackager;
import org.eclipse.iot.unide.ppmp.measurements.MeasurementsWrapper;

import com.google.common.io.MoreFiles;

public class Converter {

    private static Processor processor = new Processor();

    public static void main(final String[] args) throws IOException {
        try (
                Writer wrt = Files.newBufferedWriter(Paths.get("/Volumes/BCX2018 Team Folders/03/Data/kafka.sql"), StandardCharsets.UTF_8);
                PrintWriter prnt = new PrintWriter(wrt);
        ) {
            list(Paths.get("/Volumes/BCX2018 Team Folders/03/Data/01_Packaging_Simulation/01_Kafka/"))
                    //.limit(5)
                    .map(Converter::read)
                    .map(Converter::toSql)
                    .forEach(prnt::println);
        }

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
