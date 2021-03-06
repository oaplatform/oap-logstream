package oap.logstream.disk;

import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static oap.testng.Asserts.assertFile;
import static oap.testng.TestDirectoryFixture.testPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;


public class LogMetadataTest extends Fixtures {
    {
        fixture( TestDirectoryFixture.FIXTURE );
    }

    @Test
    public void testSave() throws IOException {
        var file = testPath( "file" );

        var metadata = new LogMetadata( "fpp", "type", "shard", "host", Map.of(), "h1,h2" );
        metadata.writeFor( file );

        assertFile( Path.of( file.toString() + ".metadata.yaml" ) ).hasContent( """
            ---
            filePrefixPattern: "fpp"
            type: "type"
            shard: "shard"
            clientHostname: "host"
            headers: "h1,h2"
            """ );
    }

    @Test
    public void testLoadWithoutHeaders() throws IOException {
        Files.writeString( testPath( "file.gz.metadata.yaml" ), """
            ---
            filePrefixPattern: "fpp"
            type: "type"
            shard: "shard"
            clientHostname: "host"
            """ );

        var metadata = LogMetadata.readFor( testPath( "file.gz" ) );
        assertThat( metadata.headers ).isNull();
    }

    @Test
    public void testSaveLoad() {
        var file = testPath( "file" );

        var metadata = new LogMetadata( "fpp", "type", "shard", "host", Map.of(), "h1,h2" );
        metadata.writeFor( file );

        var dt = new DateTime( 2019, 11, 29, 10, 9, 0, 0, UTC );
        LogMetadata.addProperty( file, "time", dt.toString() );

        var newLm = LogMetadata.readFor( file );
        assertThat( newLm.getDateTime( "time" ) ).isEqualTo( dt );
        assertThat( newLm.shard ).isEqualTo( "shard" );
    }
}
