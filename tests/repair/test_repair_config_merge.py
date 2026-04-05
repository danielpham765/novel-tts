from __future__ import annotations

from novel_tts.crawl.repair_config import (
    ChapterRepairRule,
    RepairCandidate,
    RepairConfig,
    merge_repair_config,
)


def test_merge_repair_config_preserves_existing_defaults() -> None:
    existing = RepairConfig(
        version=1,
        generated_from=["existing.md"],
        index_gaps=[2],
        placeholder_title_suffix="skip",
        placeholder_content_zh="manual-placeholder",
        dedupe_repeated_blocks=False,
        replacements={
            10: ChapterRepairRule(
                chapter=10,
                candidates=[RepairCandidate(kind="url", source_id="ttkan", url="https://a.example/10")],
            )
        },
    )
    incoming = RepairConfig(
        version=1,
        generated_from=["new.md"],
        index_gaps=[3],
        placeholder_title_suffix="auto",
        placeholder_content_zh="auto-placeholder",
        dedupe_repeated_blocks=True,
        replacements={},
    )

    merged = merge_repair_config(existing, incoming)

    assert merged.placeholder_title_suffix == "skip"
    assert merged.placeholder_content_zh == "manual-placeholder"
    assert merged.dedupe_repeated_blocks is False
    assert merged.index_gaps == [2, 3]
    assert merged.generated_from == ["existing.md", "new.md"]
    assert 10 in merged.replacements


def test_merge_repair_config_adds_new_candidates_without_dropping_existing() -> None:
    existing = RepairConfig(
        replacements={
            12: ChapterRepairRule(
                chapter=12,
                candidates=[RepairCandidate(kind="url", source_id="ttkan", url="https://a.example/12")],
            )
        }
    )
    incoming = RepairConfig(
        replacements={
            12: ChapterRepairRule(
                chapter=12,
                candidates=[
                    RepairCandidate(kind="url", source_id="ttkan", url="https://a.example/12"),
                    RepairCandidate(kind="url", source_id="wa01", url="https://b.example/12"),
                ],
            ),
            13: ChapterRepairRule(
                chapter=13,
                candidates=[RepairCandidate(kind="url", source_id="ttkan", url="https://a.example/13")],
            ),
        }
    )

    merged = merge_repair_config(existing, incoming)

    assert list(merged.replacements.keys()) == [12, 13]
    assert [c.url for c in merged.replacements[12].candidates] == ["https://a.example/12", "https://b.example/12"]
    assert [c.url for c in merged.replacements[13].candidates] == ["https://a.example/13"]
