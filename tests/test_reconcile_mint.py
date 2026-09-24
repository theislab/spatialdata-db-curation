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
            "uid_old": "", "Species": ""}
    base.update(kw)
    return base


def _kspace(*uids, source="10x Genomics", used=()):
    """Build minimal keyspace_rows fixtures: `used` uids get a non-empty id."""
    used = set(used)
    return [{"uid": u, "source": source, "id": "reserved" if u in used else ""}
            for u in uids]


# --- load_keyspace_rows -----------------------------------------------------

def test_load_keyspace_rows_reads_semicolon(tmp_path):
    p = tmp_path / "k.csv"
    p.write_text("uid;source;id\n10aaa;10x Genomics;\n10aab;10x Genomics;used\n",
                 encoding="utf-8")
    rows = rc.load_keyspace_rows(str(p))
    assert rows == [
        {"uid": "10aaa", "source": "10x Genomics", "id": ""},
        {"uid": "10aab", "source": "10x Genomics", "id": "used"},
    ]


def test_load_uid_keyspace_still_returns_a_set(tmp_path):
    p = tmp_path / "k.csv"
    p.write_text("uid;source;id\n10aaa;10x Genomics;\n10aab;10x Genomics;used\n",
                 encoding="utf-8")
    assert rc.load_uid_keyspace(str(p)) == {"10aaa", "10aab"}


# --- free_10x_uids -----------------------------------------------------------

def test_free_10x_uids_excludes_non_10x_source():
    rows = _kspace("10aaa", source="10x Genomics") + _kspace("vgaaa", source="vizgen")
    assert rc.free_10x_uids(rows, used=set()) == ["10aaa"]


def test_free_10x_uids_excludes_already_assigned_id_and_used_set():
    rows = [
        {"uid": "10aaa", "source": "10x Genomics", "id": ""},
        {"uid": "10aab", "source": "10x Genomics", "id": "reserved"},
        {"uid": "10aac", "source": "10x Genomics", "id": ""},
    ]
    assert rc.free_10x_uids(rows, used={"10aac"}) == ["10aaa"]


def test_free_10x_uids_is_sorted():
    rows = _kspace("10bbb", "10aaa", "10ccc")
    assert rc.free_10x_uids(rows, used=set()) == ["10aaa", "10bbb", "10ccc"]


# --- mint_missing_uids --------------------------------------------------------

def test_mint_assigns_fresh_uid_to_uidless_row_and_leaves_existing_untouched():
    reg = [_reg(dataset_id="ds_a", local_uid="10aaa"),
           _reg(dataset_id="ds_b", local_uid="")]
    rows = _kspace("10aaa", "10bbb", used=("10aaa",))
    out = rc.mint_missing_uids(reg, rows)
    assert out[0]["local_uid"] == "10aaa"
    assert out[1]["local_uid"] == "10bbb"


def test_mint_produces_unique_uids_from_10x_space_never_colliding_with_existing():
    reg = [_reg(dataset_id="ds_a", local_uid="10aaa"),
           _reg(dataset_id="ds_b", local_uid=""),
           _reg(dataset_id="ds_c", local_uid="")]
    rows = _kspace("10aaa", "10bbb", "10ccc", used=("10aaa",))
    out = rc.mint_missing_uids(reg, rows)
    minted = [r["local_uid"] for r in out]
    assert minted[0] == "10aaa"
    assert len(set(minted)) == 3
    assert minted[1] != minted[2]
    assert set(minted[1:]) == {"10bbb", "10ccc"}


def test_mint_is_deterministic():
    reg = [_reg(dataset_id="ds_a", local_uid=""), _reg(dataset_id="ds_b", local_uid="")]
    rows = _kspace("10ccc", "10aaa", "10bbb")
    out1 = rc.mint_missing_uids(reg, rows)
    out2 = rc.mint_missing_uids(reg, rows)
    assert out1 == out2
    assert [r["local_uid"] for r in out1] == ["10aaa", "10bbb"]


def test_mint_raises_when_free_pool_exhausted():
    reg = [_reg(dataset_id="ds_a", local_uid=""), _reg(dataset_id="ds_b", local_uid="")]
    rows = _kspace("10aaa", used=("10aaa",))  # zero free uids
    with pytest.raises(ValueError):
        rc.mint_missing_uids(reg, rows)


def test_mint_does_not_mutate_input_registry():
    reg = [_reg(dataset_id="ds_a", local_uid="")]
    rows = _kspace("10aaa")
    rc.mint_missing_uids(reg, rows)
    assert reg[0]["local_uid"] == ""


# --- check_unique --------------------------------------------------------------

def test_check_unique_flags_duplicate_and_empty():
    reg = [_reg(dataset_id="ds_a", local_uid="10aaa"),
           _reg(dataset_id="ds_b", local_uid="10aaa"),
           _reg(dataset_id="ds_c", local_uid="")]
    offenders = rc.check_unique(reg)
    assert "10aaa" in offenders
    assert any("empty" in o.lower() or o == "" for o in offenders)


def test_check_unique_clean_registry_has_no_offenders():
    reg = [_reg(dataset_id="ds_a", local_uid="10aaa"),
           _reg(dataset_id="ds_b", local_uid="10bbb")]
    assert rc.check_unique(reg) == []


# --- reconcile wiring: everyone gets a uid ------------------------------------

def test_reconcile_mints_for_rows_that_fail_to_link():
    reg = [_reg(dataset_id="ds_a", local_uid="")]
    scrape = []  # nothing to link against
    keyspace_rows = _kspace("10aaa")
    folded, unmatched = rc.reconcile(reg, scrape, keyspace_rows)
    assert folded[0]["local_uid"] == "10aaa"
    assert unmatched == []


def test_reconcile_unmatched_report_is_always_empty_after_minting():
    reg = [_reg(dataset_id="ds_a", local_uid=""), _reg(dataset_id="ds_b", local_uid="")]
    scrape = []
    keyspace_rows = _kspace("10aaa", "10bbb")
    _, unmatched = rc.reconcile(reg, scrape, keyspace_rows)
    assert unmatched == []


def test_reconcile_mint_is_idempotent_end_to_end():
    reg = [_reg(dataset_id="ds_a", local_uid="")]
    scrape = []
    keyspace_rows = _kspace("10aaa")
    first, un1 = rc.reconcile(reg, scrape, keyspace_rows)
    second, un2 = rc.reconcile(first, scrape, keyspace_rows)
    assert first == second
    assert un1 == un2 == []
