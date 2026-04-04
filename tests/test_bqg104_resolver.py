from novel_tts.crawl.resolvers.bqg104 import Bqg104Resolver


def test_bqg104_parse_directory_builds_api_chapter_urls() -> None:
    resolver = Bqg104Resolver()

    entries = resolver.parse_directory(
        '{"list":["第1章 姐姐替嫁","第2章 神秘的种子"]}',
        "https://c1cb4a.bqg104.cc/api/booklist?id=188262",
    )

    assert sorted(entries) == [1, 2]
    assert entries[1].title == "第1章 姐姐替嫁"
    assert entries[1].url == "https://c1cb4a.bqg104.cc/api/chapter?id=188262&chapterid=1"
    assert entries[2].url == "https://c1cb4a.bqg104.cc/api/chapter?id=188262&chapterid=2"


def test_bqg104_parse_chapter_reads_json_payload() -> None:
    resolver = Bqg104Resolver()

    parsed = resolver.parse_chapter(
        '{"chaptername":"第1章 姐姐替嫁","txt":"第一行\\n\\n第二行"}',
        expected_chapter_number=1,
    )

    assert parsed.chapter_number == 1
    assert parsed.title == "第1章 姐姐替嫁"
    assert parsed.content == "第一行\n第二行"


def test_bqg104_parse_directory_remaps_shifted_chapter_ids(monkeypatch) -> None:
    resolver = Bqg104Resolver()

    def fake_load_json_url(url: str) -> dict[str, object]:
        if "api/book?" in url:
            return {"lastchapterid": "2394"}
        if "api/chapter?" in url:
            chapter_id = int(url.split("chapterid=", 1)[1])
            if chapter_id >= 2381:
                chapter_number = chapter_id - 4
            else:
                chapter_number = chapter_id
            return {"chaptername": f"第{chapter_number}章 示例"}
        raise AssertionError(url)

    monkeypatch.setattr(Bqg104Resolver, "_load_json_url", staticmethod(fake_load_json_url))

    entries = resolver.parse_directory(
        '{"list":["第2377章 A","第2378章 B","第2379章 C","第2380章 D","第2381章 E","第2382章 F"]}',
        "https://c1cb4a.bqg104.cc/api/booklist?id=188262",
    )

    assert entries[2377].url.endswith("chapterid=2381")
    assert entries[2378].url.endswith("chapterid=2382")
    assert entries[2379].url.endswith("chapterid=2383")
    assert entries[2380].url.endswith("chapterid=2384")
    assert entries[2381].url.endswith("chapterid=2385")
    assert entries[2382].url.endswith("chapterid=2386")
