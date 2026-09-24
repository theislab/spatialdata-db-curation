import reconcile_datasets as rc


def test_source_fp_normalizes_url_variants():
    a = rc.source_fp("https://www.10xgenomics.com/datasets/foo/")
    b = rc.source_fp("http://10xgenomics.com/datasets/foo?utm_source=x")
    assert a and a == b


def test_source_fp_empty_is_blank():
    assert rc.source_fp("") == ""
    assert rc.source_fp(None) == ""


def test_load_scrape_reads_semicolon(tmp_path):
    p = tmp_path / "s.csv"
    p.write_text("Datasets;uid;dataset_link\nA;10abc;https://x/y\n", encoding="utf-8")
    rows = rc.load_scrape(str(p))
    assert rows == [{"Datasets": "A", "uid": "10abc", "dataset_link": "https://x/y"}]
