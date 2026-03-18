from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QuotaRequest:
    request_id: str
    tokens: int
    rpm_req: int = 1
    rpd_req: int = 1


@dataclass(frozen=True)
class TpmEvent:
    ts: float
    tokens: int


def _purge_old_timestamps(events: list[float], *, now: float, window_seconds: float) -> list[float]:
    window_start = now - float(window_seconds)
    return [ts for ts in events if ts >= window_start]


def _purge_old_tpm(events: list[TpmEvent], *, now: float, window_seconds: float) -> list[TpmEvent]:
    window_start = now - float(window_seconds)
    return [ev for ev in events if ev.ts >= window_start]


def estimate_grant_times(
    *,
    now: float,
    rpm_limit: int,
    tpm_limit: int,
    rpd_limit: int,
    rpm_events: list[float],
    tpm_events: list[TpmEvent],
    rpd_events: list[float],
    requests: list[QuotaRequest],
) -> dict[str, float]:
    """
    Estimate FIFO grant times for queued quota requests.

    This is an *estimate* used for operator countdown display. It models each request as consuming:
    - RPM: +1 for 60s (freezed at grant time)
    - TPM: +tokens for 60s (freezed at grant time)
    - RPD: +1 for 86400s (freezed at grant time)

    It does not model commit-time extensions (t2-based locked windows). That behavior is enforced by Redis state;
    this function is only to provide a stable countdown for queued requests.
    """

    rpm = max(0, int(rpm_limit or 0))
    tpm = max(0, int(tpm_limit or 0))
    rpd = max(0, int(rpd_limit or 0))

    # Normalize existing events to active windows.
    rpm_q = sorted(_purge_old_timestamps(rpm_events, now=now, window_seconds=60.0))
    tpm_q = sorted(_purge_old_tpm(tpm_events, now=now, window_seconds=60.0), key=lambda ev: ev.ts)
    rpd_q = sorted(_purge_old_timestamps(rpd_events, now=now, window_seconds=86400.0))

    out: dict[str, float] = {}
    t = float(now)

    def rpm_used(at: float) -> int:
        if rpm <= 0:
            return 0
        active = _purge_old_timestamps(rpm_q, now=at, window_seconds=60.0)
        return len(active)

    def rpd_used(at: float) -> int:
        if rpd <= 0:
            return 0
        active = _purge_old_timestamps(rpd_q, now=at, window_seconds=86400.0)
        return len(active)

    def tpm_used(at: float) -> int:
        if tpm <= 0:
            return 0
        active = _purge_old_tpm(tpm_q, now=at, window_seconds=60.0)
        return sum(ev.tokens for ev in active)

    for req in requests:
        rid = (req.request_id or "").strip()
        tokens = max(1, int(req.tokens or 0))
        rpm_req = max(0, int(req.rpm_req or 0))
        rpd_req = max(0, int(req.rpd_req or 0))
        if not rid:
            continue
        # Find earliest t where all constraints pass.
        guard = 0
        while True:
            guard += 1
            if guard > 2000:
                # Avoid infinite loops in degenerate inputs.
                out[rid] = t
                break
            rpm_active = _purge_old_timestamps(rpm_q, now=t, window_seconds=60.0)
            rpd_active = _purge_old_timestamps(rpd_q, now=t, window_seconds=86400.0)
            tpm_active = _purge_old_tpm(tpm_q, now=t, window_seconds=60.0)
            rpm_q[:] = sorted(rpm_active)
            rpd_q[:] = sorted(rpd_active)
            tpm_q[:] = sorted(tpm_active, key=lambda ev: ev.ts)

            ok_rpm = (rpm <= 0) or (len(rpm_q) < rpm)
            ok_rpd = (rpd <= 0) or (len(rpd_q) < rpd)
            cur_tokens = sum(ev.tokens for ev in tpm_q) if tpm > 0 else 0
            ok_tpm = (tpm <= 0) or ((cur_tokens + tokens) <= tpm)
            if rpm_req <= 0:
                ok_rpm = True
            if rpd_req <= 0:
                ok_rpd = True
            if ok_rpm and ok_tpm and ok_rpd:
                out[rid] = t
                if rpm > 0 and rpm_req > 0:
                    # Model request quota as N identical events at the same grant time.
                    rpm_q.extend([t] * rpm_req)
                if rpd > 0 and rpd_req > 0:
                    rpd_q.extend([t] * rpd_req)
                if tpm > 0:
                    tpm_q.append(TpmEvent(ts=t, tokens=tokens))
                break

            next_times: list[float] = [t + 0.001]
            if rpm > 0 and rpm_req > 0 and len(rpm_q) >= rpm and rpm_q:
                need_drop = len(rpm_q) - (rpm - 1)
                cutoff = rpm_q[max(0, need_drop - 1)]
                next_times.append(cutoff + 60.0)
            if rpd > 0 and rpd_req > 0 and len(rpd_q) >= rpd and rpd_q:
                need_drop = len(rpd_q) - (rpd - 1)
                cutoff = rpd_q[max(0, need_drop - 1)]
                next_times.append(cutoff + 86400.0)
            if tpm > 0 and (cur_tokens + tokens) > tpm and tpm_q:
                need_reduce = (cur_tokens + tokens) - tpm
                reduced = 0
                cutoff_ts = tpm_q[0].ts
                for ev in tpm_q:
                    reduced += int(ev.tokens or 0)
                    cutoff_ts = ev.ts
                    if reduced >= need_reduce:
                        break
                next_times.append(cutoff_ts + 60.0)

            t = max(next_times)

    return out
