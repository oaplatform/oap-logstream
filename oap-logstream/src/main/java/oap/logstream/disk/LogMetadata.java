package oap.logstream.disk;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.io.Files;
import oap.json.Binder;
import oap.logstream.LogId;
import org.joda.time.DateTime;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.joda.time.DateTimeZone.UTC;

/**
 * Created by igor.petrenko on 2019-11-28.
 */
@ToString
@EqualsAndHashCode(exclude = "clientHostname")
public class LogMetadata {
    public final String type;
    public final String shard;
    public final String clientHostname;
    @JsonIgnore
    public final Map<String, String> properties;
    private final String filePrefixPattern;

    @JsonCreator
    public LogMetadata(String filePrefixPattern, String type, String shard, String clientHostname, Map<String, String> properties) {
        this.filePrefixPattern = filePrefixPattern;
        this.type = type;
        this.shard = shard;
        this.clientHostname = clientHostname;
        this.properties = properties != null ? properties : new HashMap<>();
    }

    public LogMetadata(LogId logId) {
        this(logId.filePrefixPattern, logId.logType, String.valueOf(logId.shard), logId.clientHostname, logId.properties);
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

    public static void addProperty(Path path, String name, String value) {
        var lm = LogMetadata.getForFile(path);
        lm.setProperty(name, value);
        lm.putForFile(path);
    }

    @JsonAnyGetter
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonAnySetter
    public void setProperty(String name, String value) {
        properties.put(name, value);
    }

    public void putForFile(Path file) {
        Binder.yaml.marshal(pathOfMetadata(file), this);
    }

    public DateTime getDateTime(String name) {
        var dt = properties.get(name);
        return dt == null ? null : new DateTime(dt, UTC);
    }

    public String getString(String name) {
        return properties.get(name);
    }
}
