package oap.logstream.template;

import oap.logstream.tsv.TsvInputStream;
import oap.template.TemplateAccumulatorString;
import oap.util.Strings;

/**
 * Created by igor.petrenko on 2020-10-20.
 * Why is this thing here?
 */
@Deprecated
public class TemplateAccumulatorClickhouse extends TemplateAccumulatorString {
    @Override
    public void accept( boolean b ) {
        super.accept( b ? "1" : "0" );
    }

    @Override
    public void accept( Enum<?> e ) {
        accept( e.name() );
    }

    @Override
    public void accept( String text ) {
        super.accept( Strings.UNKNOWN.equals( text ) ? "" : TsvInputStream.escape( text ) );
    }

    @Override
    public TemplateAccumulatorString newInstance() {
        return new TemplateAccumulatorClickhouse();
    }
}
