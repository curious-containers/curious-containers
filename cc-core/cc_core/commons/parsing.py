from cc_core.commons.exceptions import ParsingError


def _partition_all_internal(s, sep):
    """
    Uses str.partition() to split every occurrence of sep in s. The returned list does not contain empty strings.

    :param s: The string to split.
    :param sep: A separator string.
    :return: A list of parts split by sep
    """
    parts = list(s.partition(sep))

    # if sep found
    if parts[1] == sep:
        new_parts = partition_all(parts[2], sep)
        parts.pop()
        parts.extend(new_parts)
        return [p for p in parts if p]
    else:
        if parts[0]:
            return [parts[0]]
        else:
            return []


def partition_all(s, sep):
    """
    Uses str.partition() to split every occurrence of sep in s. The returned list does not contain empty strings.
    If sep is a list, all separators are evaluated.

    :param s: The string to split.
    :param sep: A separator string or a list of separator strings.
    :return: A list of parts split by sep
    """
    if isinstance(sep, list):
        parts = _partition_all_internal(s, sep[0])
        sep = sep[1:]

        for s in sep:
            tmp = []
            for p in parts:
                tmp.extend(_partition_all_internal(p, s))
            parts = tmp

        return parts
    else:
        return _partition_all_internal(s, sep)


def split_all(reference, sep):
    """
    Splits a given string at a given separator or list of separators.

    :param reference: The reference to split.
    :param sep: Separator string or list of separator strings.
    :return: A list of split strings
    """
    parts = partition_all(reference, sep)
    return [p for p in parts if p not in sep]


def strip_start_end(parts, start_symbol, end_symbol):
    """
    For every part in parts remove the start_symbol from the begin of part and the end_symbol from the end of part if
    both are present. Otherwise (if start_symbol or end_symbol is missing) leaves part unchanged.
    :param parts: A list of strings, which may start with start_symbol and end with end_symbol
    :type parts: List[str]
    :param start_symbol: The symbol to remove from the start of every part, if end_symbol is present at the end of this
    part.
    :param end_symbol: The symbol to remove from the end of every part, if start_symbol is present at the start of this
    part.
    :return: A new list containing the given parts but with removed start_symbol and end_symbol
    """
    stripped_parts = []

    for part in parts:
        if part.startswith(start_symbol) and part.endswith(end_symbol):
            stripped_part = part.lstrip(start_symbol).rstrip(end_symbol)
            stripped_parts.append(stripped_part)
        else:
            stripped_parts.append(part)

    return stripped_parts


def split_into_parts_with_separators(to_split, separator_list, remove_separators=False):
    """
    Splits the given string into parts, which either start with one of the start separators and end with the
    corresponding end separator or do not contain separators.
    If remove_separators is set to True, the parts are split in the same way, but afterwards all separators are removed
    from the resulting parts.

    :param to_split: The string to split
    :param separator_list: A list containing tuples of separator starts and ends.
    Example: [('start', 'end'), ('<', '>'), ...]
    :param remove_separators: Defines whether the separator symbols should be removed from the output.
    :raise ParsingError: If separator start is found, but the corresponding end separator does not occur until the end
    of to_split
    :return: The given string split into parts, by separators given in separator list.
    """
    # parts is a list containing strings, which will be split by the following procedure
    parts = [to_split]
    for start_separator, end_separator in separator_list:
        tmp_parts = []
        for p in parts:
            tmp_parts.extend(split_into_parts(p, start_separator, end_separator, remove_separators))
        parts = tmp_parts

    return parts


def split_into_parts(to_split, start, end, remove_separators=False):
    """
    Splits to_split in normal strings and strings, that start with the given start separator and end with the given end
    separator.
    If remove_separators is set to True, to_split is split in the same way, but separators are removed afterwards.

    Example:
    split_into_parts("a(b)cde()(fg)", start='(', end=')') == ["a", "(b)", "cde", "()", "(fg)"]

    :param to_split: The string to split
    :param start: The start sequence to search for
    :param end: The end sequence to search for
    :param remove_separators: If set to True, separators are removed from the output parts
    :raise ParsingError: If an input reference is not closed and a new reference starts or the string ends.
    :return: A list of normal strings and unresolved input references.
    """
    parts = partition_all(to_split, [start, end])

    result = []
    part = []
    in_reference = False
    for p in parts:
        if in_reference:
            if p == start:
                raise ParsingError('A new framed string has been started, although the old framed string has not yet '
                                   'been completed.\n{}'.format(to_split))
            elif p == end:
                part.append(end)
                result.append(''.join(part))
                part = []
                in_reference = False
            else:
                part.append(p)
        else:
            if p == start:
                if part:
                    result.append(''.join(part))
                part = [start]
                in_reference = True
            else:
                part.append(p)

    if in_reference:
        raise ParsingError(
            'string "{}" contains start symbol "{}" but not end symbol "{}"'.format(to_split, start, end)
        )
    elif part:
        result.append(''.join(part))

    if remove_separators:
        result = strip_start_end(result, start, end)

    return result
