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

package oap.logstream.disk;

import oap.dictionary.DictionaryParser;
import oap.dictionary.DictionaryRoot;
import oap.logstream.LogId;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.util.Dates;
import oap.util.LinkedHashMaps;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;

import static oap.dictionary.DictionaryParser.INCREMENTAL_ID_STRATEGY;
import static oap.logstream.Timestamp.BPH_12;
import static oap.logstream.formats.parquet.ParquetAssertion.assertParquet;
import static oap.logstream.formats.parquet.ParquetAssertion.row;
import static oap.testng.Asserts.pathOfTestResource;
import static org.joda.time.DateTimeZone.UTC;

public class ParquetWriterTest extends Fixtures {
    private static final String FILE_PATTERN = "${p}-file-${INTERVAL}-${LOG_VERSION}.parquet";
    private final DictionaryRoot dr;

    public ParquetWriterTest() {
        fixture( TestDirectoryFixture.FIXTURE );
        dr = DictionaryParser.parse( pathOfTestResource( getClass(), "datamodel.conf" ), INCREMENTAL_ID_STRATEGY );
    }

    @Test
    public void testWrite() {
        Dates.setTimeFixed( 2022, 3, 8, 21, 11 );

        var content1 = "s11\t21\t[1]\t2022-03-11T15:16:12\ns12\t22\t[1,2]\t2022-03-11T15:16:13";
        var bytes1 = content1.getBytes();

        var content2 = "s111\t121\t[rr]\t2022-03-11T15:16:14\ns112\t122\t[zz,66]\t2022-03-11T15:16:15";
        var bytes2 = content2.getBytes();


        var headers = "COL1\tCOL2\tCOL3\tDATETIME";
        LogId logId = new LogId( "", "TEST", "log", 0, LinkedHashMaps.of( "p", "1" ), headers );
        Path logs = TestDirectoryFixture.testPath( "logs" );
        try( var writer = new ParquetWriter( logs, dr, FILE_PATTERN, logId, 1024, BPH_12, 20 ) ) {
            writer.write( bytes1, msg -> {} );
            writer.write( bytes2, msg -> {} );
        }

        assertParquet( logs.resolve( "1-file-02-1.parquet" ) )
            .containOnlyHeaders( "COL1", "COL2", "COL3", "DATETIME" )
            .containsExactly(
                row( "s11", 21L, List.of( "1" ), s( 2022, 3, 11, 15, 16, 12 ) ),
                row( "s12", 22L, List.of( "1", "2" ), s( 2022, 3, 11, 15, 16, 13 ) ),
                row( "s111", 121L, List.of( "rr" ), s( 2022, 3, 11, 15, 16, 14 ) ),
                row( "s112", 122L, List.of( "zz", "66" ), s( 2022, 3, 11, 15, 16, 15 ) )
            );

        assertParquet( logs.resolve( "1-file-02-1.parquet" ), "COL3", "COL2" )
            .containOnlyHeaders( "COL3", "COL2" )
            .contains( row( List.of( "1" ), 21L ) );
    }

    private long s( int year, int monthOfYear, int dayOfMonth, int hourOfDay, int minuteOfHour, int secondOfMinute ) {
        return new DateTime( year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, UTC ).getMillis() / 1000;
    }
}