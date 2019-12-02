package oap.logstream.disk;

import oap.testng.Env;
import oap.testng.Fixtures;
import oap.testng.TestDirectory;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;

import static oap.testng.Asserts.assertFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Created by igor.petrenko on 2019-11-29.
 */
public class LogMetadataTest extends Fixtures {
    {
        fixture(TestDirectory.FIXTURE);
    }

    @Test
    public void testSave() {
        var file = Env.tmpPath("file");

        var lm = new LogMetadata("fpp", "type", "shard", "host", Map.of());
        lm.putForFile(file);

        assertFile(Path.of(file.toString() + ".metadata.yaml")).hasContent("""
                ---
                filePrefixPattern: "fpp"
                type: "type"
                shard: "shard"
                clientHostname: "host"
                """);
    }

    @Test
    public void testSaveLoad() {
        var file = Env.tmpPath("file");

        var lm = new LogMetadata("fpp", "type", "shard", "host", Map.of());
        lm.putForFile(file);

        var dt = new DateTime(2019, 11, 29, 10, 9, 0, 0, UTC);
        LogMetadata.addProperty(file, "time", dt.toString());

        var newLm = LogMetadata.getForFile(file);
        assertThat(newLm.getDateTime("time")).isEqualTo(dt);
        assertThat(newLm.shard).isEqualTo("shard");
    }
}
