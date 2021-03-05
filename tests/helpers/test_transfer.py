#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest

from app.helpers.transfer import calculate_ranges


@pytest.mark.parametrize(
    "size, number_parts, expected",
    [
        (1303, 4, ["0-326", "327-652", "653-977", "978-1303"]),
        (6, 6, ["0-1", "2-2", "3-3", "4-4", "5-5", "6-6"]),
        (1000, 1, ["0-1000"]),
    ],
)
def test_calculate_ranges(size, number_parts, expected):
    assert calculate_ranges(size, number_parts) == expected


@pytest.mark.parametrize(
    "size, number_parts, side_effect, message",
    [
        (1000, 0, ZeroDivisionError, "division by zero"),
        (2, 4, ValueError, "Amount of parts '4' is greater than the size '2'"),
    ],
)
def test_calculate_ranges_error(size, number_parts, side_effect, message):
    with pytest.raises(side_effect) as e:
        calculate_ranges(size, number_parts)
    assert str(e.value) == message
