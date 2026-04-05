from __future__ import annotations

from novel_tts.crawl.resolvers.ttkan import TtkanResolver


def test_ttkan_parse_chapter_filters_metadata_lines() -> None:
    resolver = TtkanResolver()
    html = """
    <html>
      <body>
        <h1>第123章 標題</h1>
        <div id="content">
          2026-03-09 11:41:07<br/>
          作者： 七柒四十九<br/>
          宋楚薇起身離開辦公室後，顧若塵拿起手機看著照片上的喬喬微微一笑。<br/>
          誰能想到以前的一個「醜小鴨」，以後有可能變成一個萬眾矚目聚光燈下的超模了。<br/>
        </div>
      </body>
    </html>
    """

    parsed = resolver.parse_chapter(html, expected_chapter_number=123)

    assert parsed.chapter_number == 123
    assert "2026-03-09 11:41:07" not in parsed.content
    assert "作者： 七柒四十九" not in parsed.content
    assert "宋楚薇起身離開辦公室後" in parsed.content
    assert "誰能想到以前的一個「醜小鴨」" in parsed.content
