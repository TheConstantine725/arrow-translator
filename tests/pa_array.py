from datetime import datetime

import pyarrow as pa


def main():
    _dt = datetime.now().astimezone()
    print(arr := pa.array([_dt for _ in range(1)]))
    print(arr.type)


if __name__ == "__main__":
    main()
