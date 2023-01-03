package oap.logstream.disk;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.Executors;
import oap.io.Files;
import oap.logstream.Timestamp;
import oap.util.Dates;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractFinisher implements Runnable {
    public static final String CORRUPTED_DIRECTORY = ".corrupted";
    public final Path sourceDirectory;
    public final long safeInterval;
    public final String mask;
    public final Path corruptedDirectory;
    private final Timestamp timestamp;
    public int threads = Runtime.getRuntime().availableProcessors();
    protected int bufferSize = 1024 * 256 * 4 * 4;


    @SneakyThrows
    protected AbstractFinisher( Path sourceDirectory, long safeInterval, String mask, Timestamp timestamp ) {
        this.sourceDirectory = sourceDirectory;
        this.safeInterval = safeInterval;
        this.mask = mask;
        this.corruptedDirectory = sourceDirectory.resolve( CORRUPTED_DIRECTORY );
        this.timestamp = timestamp;
    }

    public void start() {
        log.info( "threads = {}, sourceDirectory = {}, corruptedDirectory = {}, mask = {}, safeInterval = {}, bufferSize = {}",
            threads, sourceDirectory, corruptedDirectory, mask, Dates.durationToString( safeInterval ), bufferSize );
    }

    @Override
    public void run() {
        run( false );
    }

    @SneakyThrows
    public void run( boolean forceSync ) {
        log.debug( "let's start packing of {} in {}", mask, sourceDirectory );
        var timestampStr = timestamp.format( DateTime.now() );

        log.debug( "current timestamp is {}", timestampStr );
        var bucketStartTime = timestamp.currentBucketStartMillis();
        var elapsed = DateTimeUtils.currentTimeMillis() - bucketStartTime;
        if( elapsed < safeInterval ) {
            log.debug( "not safe to process yet ({}ms), some of the files could still be open, waiting...", elapsed );
            cleanup();
            log.debug( "packing is skipped" );
            return;
        }
        var pool = Executors.newFixedBlockingThreadPool( threads, new ThreadFactoryBuilder().setNameFormat( "finisher-%d" ).build() );
        for( Path path : Files.wildcard( sourceDirectory, mask ) ) {
            if( path.startsWith( corruptedDirectory ) ) continue;
            if( LogMetadata.isMetadata( path ) ) continue;

            timestamp.parse( path ).ifPresentOrElse( dt -> {
                if( forceSync || dt.isBefore( bucketStartTime ) ) {
                    pool.execute( () -> process( path, dt ) );
                } else log.debug( "skipping (current timestamp) {}", path );
            }, () -> log.error( "path {} does not contain a timestamp", path ) );
        }
        pool.shutdown();
        pool.awaitTermination( 20, TimeUnit.MINUTES );
        cleanup();
        log.debug( "packing is done" );
    }

    protected abstract void cleanup();

    protected abstract void process( Path path, DateTime bucketTime );
}
