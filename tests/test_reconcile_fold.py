import pytest

import reconcile_datasets as rc

FIELDS = ["status", "dataset_id", "name", "primary_source_type", "primary_source",
          "primary_fingerprint", "manufacturer", "product", "software_name",
          "software_version", "release_date", "notes", "local_uid", "Replicate"]


def _reg(**kw):
    base = {c: "" for c in FIELDS}
    base.update(kw)
    return base


def _scrape(**kw):
    base = {"Datasets": "", "Products": "", "Software": "", "Pipeline Version": "",
            "dataset_link": "", "Publish Date": "", "Replicate": "", "uid": "",
            "uid_old": "", "Species": "", "organ": "", "tech": ""}
    base.update(kw)
    return base


def test_new_dataset_folded_with_mapped_columns():
    reg = [_reg(local_uid="10aaa", dataset_id="ds_existing")]
    scrape = [_scrape(Datasets="Brain", Products="Xenium",
                      dataset_link="https://x/brain", uid="10bbb",
                      Species="Human", organ="brain", Replicate="2")]
    out = rc.fold_new(reg, scrape)
    assert len(out) == 2
    new = out[1]
    assert new["local_uid"] == "10bbb"
    assert new["name"] == "Brain"
    assert new["product"] == "Xenium"
    assert new["manufacturer"] == "10x Genomics"
    assert new["primary_source_type"] == "url"
    assert new["dataset_id"].startswith("ds_") and len(new["dataset_id"]) == 15
    assert "Species=Human" in new["notes"] and "organ=brain" in new["notes"]
    assert new["Replicate"] == "2"
    assert set(new.keys()) == set(FIELDS)  # no extra columns leaked in


def test_uid_already_in_registry_not_refolded():
    reg = [_reg(local_uid="10bbb")]
    scrape = [_scrape(dataset_link="https://x/brain", uid="10bbb")]
    assert len(rc.fold_new(reg, scrape)) == 1


def test_duplicate_scrape_uid_folds_once():
    reg = []
    scrape = [_scrape(dataset_link="https://x/a", uid="10bbb", uid_old="10old"),
              _scrape(dataset_link="https://x/a2", uid="10bbb", uid_old="10old2")]
    # non-empty registry (needed to supply the column schema); scrape has two
    # rows sharing uid="10bbb" with different uid_old values
    out = rc.fold_new([_reg(local_uid="10aaa")], scrape)
    new_uids = [r["local_uid"] for r in out if r["local_uid"] == "10bbb"]
    assert len(new_uids) == 1


def test_fold_new_empty_registry_raises():
    with pytest.raises(ValueError):
        rc.fold_new([], [_scrape(dataset_link="https://x/a", uid="10bbb")])
