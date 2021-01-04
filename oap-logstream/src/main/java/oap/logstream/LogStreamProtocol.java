package oap.logstream;

import oap.message.MessageProtocol;


public interface LogStreamProtocol {
    byte MESSAGE_TYPE = 20;
    short STATUS_BACKEND_LOGGER_NOT_AVAILABLE = 20000;
    short STATUS_OK = MessageProtocol.STATUS_OK;
}
