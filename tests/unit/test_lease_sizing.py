import pytest

from quota.service import lease_chunk


@pytest.mark.parametrize("limit, chunk", [(500, 25), (100, 5), (10, 1), (1, 1), (0, 1), (1001, 51)])
def test_lease_chunk_is_5_percent_rounded_up_and_at_least_one(limit, chunk):
    assert lease_chunk(limit, 0.05) == chunk
