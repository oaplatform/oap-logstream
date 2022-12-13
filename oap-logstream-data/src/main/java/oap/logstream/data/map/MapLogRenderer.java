package oap.logstream.data.map;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.logstream.data.LogRenderer;
import oap.reflect.Reflect;
import oap.template.TemplateAccumulatorString;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static oap.logstream.data.TsvDataTransformer.ofBoolean;
import static oap.logstream.data.TsvDataTransformer.ofString;

@ToString
@EqualsAndHashCode
public class MapLogRenderer implements LogRenderer<Map<String, Object>, String, StringBuilder, TemplateAccumulatorString> {
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
    public byte[] render( @Nonnull Map<String, Object> data ) {
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
        String line = joiner.toString() + "\n";
        return line.getBytes( UTF_8 );
    }

    @Override
    public byte[] render( @Nonnull Map<String, Object> data, StringBuilder sb ) {
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

        sb.append( joiner );
        sb.append( "\n" );
        return sb.toString().getBytes( UTF_8 );
    }
}
