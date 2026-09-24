import reconcile_datasets as rc


def _scrape(**kw):
    base = {"Datasets": "", "dataset_link": "", "Replicate": "", "uid": ""}
    base.update(kw)
    return base


def _reg(**kw):
    base = {"dataset_id": "", "primary_source": "", "primary_fingerprint": "",
            "Replicate": "", "local_uid": ""}
    base.update(kw)
    return base


def test_backfill_matches_by_fingerprint_and_replicate():
    scrape = [_scrape(dataset_link="https://www.10xgenomics.com/datasets/a/",
                      Replicate="1", uid="10abc")]
    reg = [_reg(primary_source="http://10xgenomics.com/datasets/a", Replicate="1")]
    out, unmatched = rc.backfill_uids(reg, scrape)
    assert out[0]["local_uid"] == "10abc"
    assert unmatched == []


def test_empty_replicate_is_unmatched_not_guessed():
    scrape = [_scrape(dataset_link="https://x/a", Replicate="", uid="10abc")]
    reg = [_reg(primary_source="https://x/a", Replicate="")]
    out, unmatched = rc.backfill_uids(reg, scrape)
    assert out[0]["local_uid"] == ""
    assert len(unmatched) == 1


def test_ambiguous_key_is_unmatched():
    scrape = [_scrape(dataset_link="https://x/a", Replicate="1", uid="10abc"),
              _scrape(dataset_link="https://x/a", Replicate="1", uid="10def")]
    reg = [_reg(primary_source="https://x/a", Replicate="1")]
    out, unmatched = rc.backfill_uids(reg, scrape)
    assert out[0]["local_uid"] == ""
    assert len(unmatched) == 1


def test_existing_uid_never_overwritten():
    scrape = [_scrape(dataset_link="https://x/a", Replicate="1", uid="10zzz")]
    reg = [_reg(primary_source="https://x/a", Replicate="1", local_uid="10abc")]
    out, unmatched = rc.backfill_uids(reg, scrape)
    assert out[0]["local_uid"] == "10abc"
    assert unmatched == []
