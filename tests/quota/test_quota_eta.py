from __future__ import annotations

from novel_tts.quota.eta import QuotaRequest, TpmEvent, estimate_grant_times


def test_estimate_grant_times_rpm_blocks_until_oldest_expires() -> None:
    now = 100.0
    # rpm_limit=2, two recent events -> next grant after the oldest falls out of 60s window.
    rpm_events = [95.0, 96.0]
    out = estimate_grant_times(
        now=now,
        rpm_limit=2,
        tpm_limit=0,
        rpd_limit=0,
        rpm_events=rpm_events,
        tpm_events=[],
        rpd_events=[],
        requests=[QuotaRequest(request_id="r1", tokens=1, rpm_req=1, rpd_req=0)],
    )
    # Oldest cutoff=95 => 95+60=155; inclusive window means we need to go slightly past 155.
    assert out["r1"] > 155.0
    assert out["r1"] < 155.1


def test_estimate_grant_times_tpm_blocks_until_enough_tokens_expire() -> None:
    now = 100.0
    tpm_events = [
        TpmEvent(ts=90.0, tokens=10),
        TpmEvent(ts=91.0, tokens=15),
    ]
    # tpm_limit=20, current=25, request=5 => need reduce 10 tokens => wait for first event (10 tokens) to expire.
    out = estimate_grant_times(
        now=now,
        rpm_limit=0,
        tpm_limit=20,
        rpd_limit=0,
        rpm_events=[],
        tpm_events=tpm_events,
        rpd_events=[],
        requests=[QuotaRequest(request_id="r1", tokens=5, rpm_req=0, rpd_req=0)],
    )
    # cutoff=90 => 150; inclusive window means slightly past 150.
    assert out["r1"] > 150.0
    assert out["r1"] < 150.1


def test_estimate_grant_times_fifo_accumulates_requests() -> None:
    now = 100.0
    out = estimate_grant_times(
        now=now,
        rpm_limit=1,
        tpm_limit=100,
        rpd_limit=0,
        rpm_events=[],
        tpm_events=[],
        rpd_events=[],
        requests=[
            QuotaRequest(request_id="a", tokens=10, rpm_req=1, rpd_req=0),
            QuotaRequest(request_id="b", tokens=10, rpm_req=1, rpd_req=0),
        ],
    )
    # With rpm_limit=1, second request must wait ~60s after the first.
    assert out["a"] == now
    assert out["b"] > now + 60.0
