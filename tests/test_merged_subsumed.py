import check_merged_subsumed as cm


def test_no_extra_info_when_software_covered():
    merged = [{"dataset_id": "ds_a", "Software": "Space Ranger v2.1.0"}]
    reg = [{"dataset_id": "ds_a", "software_name": "Space Ranger", "software_version": "2.1.0"}]
    assert cm.extra_info(merged, reg) == []


def test_extra_info_flags_genuine_version_mismatch():
    merged = [{"dataset_id": "ds_x", "Software": "Space Ranger v1.0.0"}]
    reg = [{"dataset_id": "ds_x", "software_name": "Space Ranger", "software_version": "1.1.0"}]
    assert cm.extra_info(merged, reg) == ["ds_x"]


def test_empty_merged_software_not_flagged():
    merged = [{"dataset_id": "ds_b", "Software": ""}]
    reg = [{"dataset_id": "ds_b", "software_name": "Space Ranger", "software_version": "2.1.0"}]
    assert cm.extra_info(merged, reg) == []


def test_dataset_id_group_conflict_flagged_even_when_last_row_matches():
    # Regression for the unsound last-wins {dataset_id: row} join: dataset_id
    # is not unique, so two registry rows can share it with different
    # software_version. A merged row with no local_uid that only matches the
    # LAST row in iteration order must still be flagged because an EARLIER
    # row in the same group disagrees. This fails against the old last-wins
    # dict (which would only see the matching last row) and passes once every
    # row sharing the dataset_id is required to cover it.
    reg = [
        {"dataset_id": "ds_r", "software_name": "Space Ranger", "software_version": "1.0.0", "local_uid": "u1"},
        {"dataset_id": "ds_r", "software_name": "Space Ranger", "software_version": "2.0.0", "local_uid": "u2"},
    ]
    merged = [{"dataset_id": "ds_r", "Software": "Space Ranger v2.0.0"}]
    assert cm.extra_info(merged, reg) == ["ds_r"]


def test_local_uid_join_uses_matched_row_not_others_in_group():
    reg = [
        {"dataset_id": "ds_s", "software_name": "Space Ranger", "software_version": "1.0.0", "local_uid": "u1"},
        {"dataset_id": "ds_s", "software_name": "Space Ranger", "software_version": "2.0.0", "local_uid": "u2"},
    ]
    covered = [{"dataset_id": "ds_s", "local_uid": "u2", "Software": "Space Ranger v2.0.0"}]
    assert cm.extra_info(covered, reg) == []

    conflicting = [{"dataset_id": "ds_s", "local_uid": "u1", "Software": "Space Ranger v2.0.0"}]
    assert cm.extra_info(conflicting, reg) == ["ds_s"]


def test_missing_registry_software_field_flags():
    reg = [{"dataset_id": "ds_e", "software_name": "", "software_version": "1.0.0", "local_uid": "u3"}]
    merged = [{"dataset_id": "ds_e", "local_uid": "u3", "Software": "Space Ranger v1.0.0"}]
    assert cm.extra_info(merged, reg) == ["ds_e"]


def test_merged_dataset_id_with_no_registry_match_flags():
    reg = [{"dataset_id": "ds_other", "software_name": "Space Ranger", "software_version": "1.0.0"}]
    merged = [{"dataset_id": "ds_missing", "Software": "Space Ranger v1.0.0"}]
    assert cm.extra_info(merged, reg) == ["ds_missing"]
