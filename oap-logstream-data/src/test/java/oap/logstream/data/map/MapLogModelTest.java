/*
 *
 *  * Copyright (c) Xenoss
 *  * Unauthorized copying of this file, via any medium is strictly prohibited
 *  * Proprietary and confidential
 *
 *
 */

package oap.logstream.data.map;

import org.testng.annotations.Test;

import java.util.Map;

import static oap.testng.Asserts.assertString;
import static oap.testng.Asserts.pathOfTestResource;

public class MapLogModelTest {
    @Test
    public void render() {
        MapLogModel dataModel = new MapLogModel( pathOfTestResource( getClass(), "datamodel.conf" ) );
        MapLogRenderer renderer = dataModel.renderer( "EVENT1", "LOG" );
        assertString( renderer.headers() ).isEqualTo( "NAME\tVALUE1\tVALUE2" );
        assertString( renderer.render( Map.of( "name", "n", "value1", "v", "value2", 2 ) ) )
            .isEqualTo( "n\tv\t2" );
    }
}
