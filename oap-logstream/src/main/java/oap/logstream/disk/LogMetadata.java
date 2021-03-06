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
import oap.util.Maps;
import org.joda.time.DateTime;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.joda.time.DateTimeZone.UTC;


@ToString
@EqualsAndHashCode( exclude = "clientHostname" )
public class LogMetadata {
    public static final String EXTENSION = ".metadata.yaml";

    public final String type;
    public final String shard;
    public final String clientHostname;
    @JsonIgnore
    public final Map<String, String> properties;
    public final String headers;
    private final String filePrefixPattern;

    @JsonCreator
    public LogMetadata( String filePrefixPattern,
                        String type,
                        String shard,
                        String clientHostname,
                        Map<String, String> properties,
                        String headers ) {
        this.filePrefixPattern = filePrefixPattern;
        this.type = type;
        this.shard = shard;
        this.clientHostname = clientHostname;
        this.properties = properties != null ? new LinkedHashMap<>( properties ) : new LinkedHashMap<>();
        this.headers = headers;
    }

    public LogMetadata( LogId logId ) {
        this( logId.filePrefixPattern, logId.logType, String.valueOf( logId.shard ),
            logId.clientHostname, logId.properties, logId.headers );
    }

    public static LogMetadata readFor( Path file ) {
        return Binder.yaml.unmarshal( LogMetadata.class, pathFor( file ) );
    }

    public static Path pathFor( Path file ) {
        return Path.of( file.toString() + EXTENSION );
    }

    public static boolean isMetadata( Path filename ) {
        return filename.toString().endsWith( EXTENSION );
    }

    public static void rename( Path filename, Path newFile ) {
        var from = pathFor( filename );
        if( Files.exists( from ) )
            Files.rename( from, pathFor( newFile ) );
    }

    public static void addProperty( Path path, String name, String value ) {
        var metadata = LogMetadata.readFor( path );
        metadata.setProperty( name, value );
        metadata.writeFor( path );
    }

    @JsonAnyGetter
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonAnySetter
    public void setProperty( String name, String value ) {
        properties.put( name, value );
    }

    public void writeFor( Path file ) {
        Binder.yaml.marshal( pathFor( file ), this );
    }

    public DateTime getDateTime( String name ) {
        return Maps.get( properties, name )
            .map( v -> new DateTime( v, UTC ) )
            .orElse( null );
    }

    public String getString( String name ) {
        return properties.get( name );
    }
}
