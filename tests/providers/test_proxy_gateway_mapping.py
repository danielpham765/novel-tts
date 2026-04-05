from __future__ import annotations

from novel_tts.net.proxy_gateway import select_proxy_for_key_index


def test_proxy_mapping_keys_per_proxy_3_wraparound() -> None:
    proxies = ["p1", "p2"]
    assert select_proxy_for_key_index(key_index=1, proxies=proxies, keys_per_proxy=3) is None
    assert select_proxy_for_key_index(key_index=2, proxies=proxies, keys_per_proxy=3) == "p1"
    assert select_proxy_for_key_index(key_index=3, proxies=proxies, keys_per_proxy=3) == "p1"
    assert select_proxy_for_key_index(key_index=4, proxies=proxies, keys_per_proxy=3) == "p1"
    assert select_proxy_for_key_index(key_index=5, proxies=proxies, keys_per_proxy=3) == "p2"
    assert select_proxy_for_key_index(key_index=6, proxies=proxies, keys_per_proxy=3) == "p2"
    assert select_proxy_for_key_index(key_index=7, proxies=proxies, keys_per_proxy=3) == "p2"
    # Round 2+: one key per proxy (round robin)
    assert select_proxy_for_key_index(key_index=8, proxies=proxies, keys_per_proxy=3) == "p1"
    assert select_proxy_for_key_index(key_index=9, proxies=proxies, keys_per_proxy=3) == "p2"
    assert select_proxy_for_key_index(key_index=10, proxies=proxies, keys_per_proxy=3) == "p1"


def test_proxy_mapping_empty_or_invalid() -> None:
    assert select_proxy_for_key_index(key_index=1, proxies=[], keys_per_proxy=3) is None
    assert select_proxy_for_key_index(key_index=0, proxies=["p1"], keys_per_proxy=3) is None
    assert select_proxy_for_key_index(key_index=-1, proxies=["p1"], keys_per_proxy=3) is None
