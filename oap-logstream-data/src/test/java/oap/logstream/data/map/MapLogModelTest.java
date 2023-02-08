/*
 *
 *  * Copyright (c) Xenoss
 *  * Unauthorized copying of this file, via any medium is strictly prohibited
 *  * Proprietary and confidential
 *
 *
 */

package oap.logstream.data.map;

import oap.dictionary.DictionaryRoot;
import org.testng.annotations.Test;

import java.util.Map;

import static oap.testng.Asserts.assertString;
import static oap.testng.Asserts.objectOfTestResource;

public class MapLogModelTest {
    @Test
    public void renderToString() {
        MapLogModel dataModel = new MapLogModel( objectOfTestResource( DictionaryRoot.class, getClass(), "datamodel.conf" ) );
        MapLogRenderer renderer = dataModel.renderer( "EVENT1", "LOG" );
        assertString( renderer.headers() ).isEqualTo( "NAME\tVALUE1\tVALUE2" );
        assertString( renderer.render( Map.of( "name", "n", "value1", "v", "value2", 2 ) ) )
            .isEqualTo( "n\tv\t2" );
    }

    @Test
    public void renderToStringBuilder() {
        MapLogModel dataModel = new MapLogModel( objectOfTestResource( DictionaryRoot.class, getClass(), "datamodel.conf" ) );
        MapLogRenderer renderer = dataModel.renderer( "EVENT1", "LOG" );
        assertString( renderer.headers() ).isEqualTo( "NAME\tVALUE1\tVALUE2" );
        StringBuilder appender = new StringBuilder( "<init>" );
        renderer.render( Map.of( "name", "n", "value1", "v", "value2", 2 ), appender );
        appender.append( "<done>" );
        assertString( appender.toString() ).isEqualTo( "<init>n\tv\t2<done>" );
    }
}
