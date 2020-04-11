import re
from typing import Generator, Any, Callable, Type, Iterable


def traverse(obj: Any) -> Generator:
    if type(obj) in [list, tuple, set]:
        for v in obj:
            yield from traverse(v)
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from traverse(v)
    else:
        yield obj


def search_and_apply(obj: Any, fce: Callable, allowed_types: Iterable[Type] = None, filter: Callable = None):

    def _is_valid(v):
        if allowed_types is not None and not any(isinstance(v, t) for t in allowed_types):
            return False
        if filter is not None and not filter(v):
            return False
        return True

    def _traverse(o):
        if type(o) in [list, tuple, set]:
            for i, v in enumerate(o):
                if not _traverse(v) and _is_valid(v):
                    o[i] = fce(v)
            return True
        if isinstance(o, dict):
            for k, v in o.items():
                if not _traverse(v) and _is_valid(v):
                    o[k] = fce(v)
            return True
        return False

    _traverse(obj)


def search_and_replace_placeholders(obj, replacements):
    def _replace(match):
        placeholder = match.group(1)
        if isinstance(replacements, dict):
            if placeholder not in replacements:
                raise ValueError(f'No data for placeholder `{placeholder}`')
            return str(replacements[placeholder])

        if not hasattr(replacements, placeholder):
            raise ValueError(f'No data for placeholder `{placeholder}`')
        return str(getattr(replacements, placeholder))

    def _apply(string):
        return re.sub(r'{(.*?)}', _replace, string)

    search_and_apply(obj, fce=_apply, allowed_types=(str,))
