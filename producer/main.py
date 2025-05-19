import pathlib
import sys

from producer.processor import Processor
from shared.middleware import spark


def main(input_path: pathlib.Path, output_path: pathlib.Path):
    p = Processor(spark=spark)
    p.process_data(input_path=input_path, output_path=output_path)


if __name__ == "__main__":
    input_path = pathlib.Path(sys.argv[1])
    output_path = pathlib.Path(sys.argv[2])

    if not input_path.exists():
        raise FileNotFoundError(f"No file found at path {input_path}")

    if not output_path.is_dir():
        raise ValueError(f"The value {output_path} is not a directory")

    main(input_path=input_path, output_path=output_path)
