package oap.logstream.data.map;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.logstream.data.LogRenderer;
import oap.reflect.Reflect;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static oap.logstream.data.TsvDataTransformer.ofBoolean;
import static oap.logstream.data.TsvDataTransformer.ofString;

@ToString
@EqualsAndHashCode
public class MapLogRenderer implements LogRenderer<Map<String, Object>> {
    private final String headers;
    private final List<String> expressions;

    public MapLogRenderer( String headers, List<String> expressions ) {
        this.headers = headers;
        this.expressions = expressions;
    }

    @Nonnull
    @Override
    public String headers() {
        return headers;
    }

    @Nonnull
    @Override
    public String render( @Nonnull Map<String, Object> data ) {
        StringJoiner joiner = new StringJoiner( "\t" );
        for( String expression : expressions ) {
            Object v = Reflect.get( data, expression );
            joiner.add( v == null
                ? ""
                : v instanceof String
                    ? ofString( ( String ) v )
                    : v instanceof Boolean
                        ? ofBoolean( ( boolean ) v )
                        : String.valueOf( v ) );
        }
        return joiner.toString();
    }
}
