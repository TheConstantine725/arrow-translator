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
                record.created,
            )
            .astimezone()
            .isoformat(),
            "level": record.levelname,
            "name": record.name,
            "function": record.funcName,
            "module": record.module,
            "line_number": record.lineno,
            "message": record.getMessage().split("|")[-1],
        }

        for _ in record.__dict__.keys():
            if _ not in PARAMETERS:
                message[_] = getattr(record, _)

        if record.exc_info:
            message["exception_message"] = self.formatException(record.exc_info)
        if record.stack_info:
            message["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(message)


class ConsoleFormatter(Formatter):
    def format(self, record: LogRecord) -> str:
        _dt = datetime.datetime.fromtimestamp(record.created).astimezone().isoformat()
        _lvl = record.levelname
        _name = record.name
        _func = record.funcName
        _mod = record.module
        _line_no = record.lineno
        _msg = record.getMessage().split("|")[-1]

        message = f"{_dt}|[{_lvl}]|[{_name}]|({_func} on {_mod}:{_line_no})|{_msg}"
        if record.exc_info:
            message += f"|Exception: {self.formatException(record.exc_info)}"
        if record.stack_info:
            message += f"|StackTrace: {self.formatStack(record.stack_info)}"

        return message
