from novel_tts.crawl.resolvers.ixdzs import IxdzsResolver


def test_ixdzs_parse_directory_expands_latest_chapter_range() -> None:
    resolver = IxdzsResolver()
    html = """
    <html><head>
      <meta property="og:novel:latest_chapter_url" content="https://ixdzs.hk/read/545646/p1556.html" />
    </head></html>
    """

    entries = resolver.parse_directory(html, "https://ixdzs.hk/read/545646/")

    assert sorted(entries)[:3] == [1, 2, 3]
    assert max(entries) == 1556
    assert entries[1].url == "https://ixdzs.hk/read/545646/p1.html"
    assert entries[1556].url == "https://ixdzs.hk/read/545646/p1556.html"


def test_ixdzs_parse_chapter_reads_article_paragraphs() -> None:
    resolver = IxdzsResolver()
    html = """
    <html><body>
      <div class="page-d-top"><h1 class="page-d-name">第1章 深蓝系统！新手礼包！</h1></div>
      <article class="page-content">
        <section>
          <p class="abg">广告</p>
          <p>第一段</p>
          <p>第二段</p>
          <p>上一章</p>
        </section>
      </article>
    </body></html>
    """

    parsed = resolver.parse_chapter(html, expected_chapter_number=1)

    assert parsed.chapter_number == 1
    assert parsed.title == "第1章 深蓝系统！新手礼包！"
    assert parsed.content == "第一段\n第二段"
