import json
from typing import IO, Any, Union

import orjson


def dumps(data: Any, sort_keys: bool = False, indent: int = None, as_bytes: bool = False) -> str:
    orjson.OPT_SORT_KEYS
    option = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NON_STR_KEYS
    if sort_keys:
        option |= orjson.OPT_SORT_KEYS
    if indent:
        assert indent == 2, f'The only supported indent is 2, given {indent=}'
        option |= orjson.OPT_INDENT_2
    dumped = orjson.dumps(data, option=option)
    if not as_bytes:
        dumped = dumped.decode()
    return dumped


def dump(data: Any, fp: IO[str], sort_keys: bool = False, indent: int = None, as_bytes: bool = False):
    fp.write(
        dumps(
            data,
            sort_keys=sort_keys,
            indent=indent,
            as_bytes=as_bytes,
        )
    )


def loads(s: Union[str, bytes]) -> Any:
    return orjson.loads(s)


def load(fp) -> Any:
    return orjson.loads(fp.read())
