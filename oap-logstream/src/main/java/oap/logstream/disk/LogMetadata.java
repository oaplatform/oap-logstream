package oap.logstream.disk;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.ToString;
import oap.io.Files;
import oap.json.Binder;
import oap.logstream.LogId;

import java.nio.file.Path;

/**
 * Created by igor.petrenko on 2019-11-28.
 */
@ToString
public class LogMetadata {
    public final String type;
    public final int shard;
    public final String clientHostname;

    @JsonCreator
    public LogMetadata(String type, int shard, String clientHostname) {
        this.type = type;
        this.shard = shard;
        this.clientHostname = clientHostname;
    }

    public LogMetadata(LogId logId) {
        this(logId.logType, logId.shard, logId.clientHostname);
    }

    public static LogMetadata getForFile(Path file) {
        return Binder.yaml.unmarshal(LogMetadata.class, pathOfMetadata(file));
    }

    private static Path pathOfMetadata(Path file) {
        return Path.of(file.toString() + ".metadata.yaml");
    }

    public static void rename(Path filename, Path newFile) {
        var from = pathOfMetadata(filename);
        if (Files.exists(from))
            Files.rename(from, pathOfMetadata(newFile));
    }

    public void putForFile(Path file) {
        Binder.yaml.marshal(pathOfMetadata(file), this);
    }
}
