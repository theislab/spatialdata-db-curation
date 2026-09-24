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
    used = set(used)
    return [{"uid": u, "source": source, "id": "reserved" if u in used else ""}
            for u in uids]


def test_reconcile_rejects_uid_outside_keyspace():
    reg = [_reg(local_uid="10zzz", dataset_id="ds_x")]
    try:
        rc.reconcile(reg, [], _kspace("10aaa"))
        assert False, "expected ValueError"
    except ValueError as e:
        assert "10zzz" in str(e)


def test_reconcile_is_idempotent():
    reg = [_reg(local_uid="10aaa", dataset_id="ds_a", primary_source="https://x/a",
                Replicate="1")]
    scrape = [_scrape(dataset_link="https://x/b", Replicate="1", uid="10bbb",
                      Datasets="B")]
    keyspace_rows = _kspace("10aaa", "10bbb")
    first, un1 = rc.reconcile(reg, scrape, keyspace_rows)
    second, un2 = rc.reconcile(first, scrape, keyspace_rows)
    assert first == second
    assert un1 == un2
