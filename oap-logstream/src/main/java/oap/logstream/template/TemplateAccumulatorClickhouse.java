package oap.logstream.template;

import oap.template.TemplateAccumulatorString;
import oap.tsv.Printer;
import oap.util.Strings;

@Deprecated
public class TemplateAccumulatorClickhouse extends TemplateAccumulatorString {
    @Override
    public void accept( boolean b ) {
        super.accept( b ? "1" : "0" );
    }

    @Override
    public void accept( String text ) {
        super.accept( Strings.UNKNOWN.equals( text ) ? "" : Printer.escape( text, false ) );
    }

    @Override
    public TemplateAccumulatorString newInstance() {
        return new TemplateAccumulatorString();
    }
}
