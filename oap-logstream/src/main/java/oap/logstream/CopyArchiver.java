/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package oap.logstream;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import oap.io.Files;
import oap.io.IoStreams;

import java.nio.file.Path;

@Slf4j
public class CopyArchiver extends Archiver {
    private static Timer logstreamArchiveTimer = Metrics.timer("logstream_archive");

    public final Path destinationDirectory;

    public CopyArchiver(Path sourceDirectory,
                        Path destinationDirectory,
                        long safeInterval,
                        String mask,
                        IoStreams.Encoding encoding,
                        Timestamp timestamp) {

        super(sourceDirectory, safeInterval, mask, encoding, timestamp);
        this.destinationDirectory = destinationDirectory;
    }

    @Override
    protected void cleanup() {
        Files.deleteEmptyDirectories(sourceDirectory, false);
        Files.deleteEmptyDirectories(destinationDirectory, false);
    }

    @Override
    protected void archive(Path path) {
        var from = IoStreams.Encoding.from(path);

        var destination = destinationDirectory.resolve(
                encoding.resolve(sourceDirectory.relativize(path)));

        logstreamArchiveTimer.record(() -> {
            if (!Files.isFileEncodingValid(path)) {
                Files.rename(path, corruptedDirectory.resolve(sourceDirectory.relativize(path)));
                log.debug("corrupted {}", path);
            } else if (from != encoding) {
                log.debug("compressing {} ({} bytes)", path, path.toFile().length());
                Files.copy(path, from, destination, encoding, bufferSize);
                log.debug("compressed {} ({} bytes)", path, destination.toFile().length());
                Files.delete(path);
            } else {
                log.debug("moving {} ({} bytes)", path, path.toFile().length());
                Files.rename(path, destination);
            }
        });
    }
}
