/**
 * Copyright
 */
package oap.logstream;

import java.util.Map;

public class NullLoggerBackend extends AbstractLoggerBackend {
    @Override
    public void log( String hostName, String filePreffix, Map<String, String> properties, String logType, String logSchemaId,
                     int shard, String headers, byte[] buffer, int offset, int length ) {

    }

    @Override
    public void close() {

    }

    @Override
    public AvailabilityReport availabilityReport() {
        return new AvailabilityReport( AvailabilityReport.State.OPERATIONAL );
    }
}
