import pathlib

_root = pathlib.Path(__file__).parents[1] / "data_store"
DATA = _root / "transactions" / "gold"
DQ = _root / "dq" / "transactions"
