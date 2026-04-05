from novel_tts.dev.context_map import load_context_map, render_overview, render_task, validate_context_map


def test_context_map_is_valid() -> None:
    payload = load_context_map()
    validate_context_map(payload)


def test_context_map_translate_task_is_narrow_and_deterministic() -> None:
    payload = load_context_map()
    rendered = render_task(payload, "translate")
    assert "Task: translate" in rendered
    assert "novel_tts/translate/novel.py" in rendered
    assert "novel_tts/queue/translation_queue.py" in rendered
    assert "Do not scan:" in rendered


def test_context_map_overview_lists_tasks() -> None:
    payload = load_context_map()
    rendered = render_overview(payload)
    assert "Stable repo context:" in rendered
    assert "- translate:" in rendered
    assert "- queue:" in rendered
