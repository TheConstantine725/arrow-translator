import datetime
import json
from logging import Formatter, LogRecord

PARAMETERS = {
    "asctime",
    "exc_info",
    "msecs",
    "exc_text",
    "created",
    "stack_info",
    "thread",
    "relativeCreated",
    "process",
    "module",
    "pathname",
    "funcName",
    "msg",
    "lineno",
    "taskName",
    "name",
    "args",
    "levelname",
    "levelno",
    "threadName",
    "filename",
    "processName",
}

class JsonLinesFormatter(Formatter):
    def format(self, record: LogRecord) -> str:
        message = {
            "timestamp": datetime.datetime.fromtimestamp(
                record.created, tz=datetime.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "function": record.funcName,
            "module": record.module,
            "line_number": record.lineno,
            "message": record.getMessage(),
        }

        for _ in record.__dict__.keys():
            if _ not in PARAMETERS:
                message[_] = getattr(record, _)

        if record.exc_info:
            message["exception_message"] = self.formatException(record.exc_info)
        if record.stack_info:
            message["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(message)
