package oap.logstream;

import oap.message.MessageProtocol;


public final class LogStreamProtocol {
    public static final byte MESSAGE_TYPE = 20;
    public static final short STATUS_BACKEND_LOGGER_NOT_AVAILABLE = 20000;
    public static final short STATUS_OK = MessageProtocol.STATUS_OK;
}
