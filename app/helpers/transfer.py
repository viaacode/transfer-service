#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List


def calculate_ranges(size_bytes: int, number_parts: int) -> List[str]:
    """Split the filesize up in multiple ranges.

    Args:
        size_bytes: The size of the file in bytes.
        number_parts: The amount of parts.

    Returns:
        List of ranges, with the range in format "{x}-{y}".
        With x and y integers and x <= y.
        Format of list: ["0-{x}", "{x+1}-{y}", ... ,"{z+1}-{size_bytes}"].

    Raises:
        ValueError: If the amount of parts is greater than the size.
    """
    if number_parts > size_bytes:
        raise ValueError(
            f"Amount of parts '{number_parts}' is greater than the size '{size_bytes}'"
        )
    ranges = []
    part_size = size_bytes / number_parts
    for i in range(number_parts):
        if i == 0:
            ranges.append(f"{i}-{round(part_size)}")
        else:
            ranges.append(f"{round(i * part_size) + 1}-{round(((i + 1) * part_size))}")
    return ranges
