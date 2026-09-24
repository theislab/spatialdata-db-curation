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
