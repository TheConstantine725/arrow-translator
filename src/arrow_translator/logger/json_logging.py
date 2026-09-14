import datetime
import json
from logging import Formatter, Handler, LogRecord


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

        if hasattr(record, "batch_size"):
            message["batch_size"] = getattr(record, "batch_size")
        if hasattr(record, "number_of_rows"):
            message["number_of_rows"] = getattr(record, "number_of_rows")

        if record.exc_info:
            message["exception_message"] = self.formatException(record.exc_info)
        return json.dumps(message)
