from datetime import datetime

import pyarrow as pa


def main():
    _dt = datetime.now().astimezone()
    print(arr := pa.repeat(_dt, 100))
    print(arr.type)


if __name__ == "__main__":
    main()
