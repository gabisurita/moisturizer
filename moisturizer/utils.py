
PRIMITIVES = [int, bool, float, str, dict, list, type(None)]


def flatten_dict(nested, separator='.'):
    def items():
        for key, value in nested.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(value).items():
                    yield "{}{}{}".format(key,
                                          separator,
                                          subkey), subvalue
            else:
                yield key, value

    return dict(items())


def unflatten_dict(flatten, separator='.'):
    unflatten = {}

    for key, value in flatten.items():
        parts = key.split(separator)
        d = unflatten

        for part in parts[:-1]:
            if part not in d:
                d[part] = dict()

            d = d[part]

        d[parts[-1]] = value

    return unflatten
