
def split_success_and_failed(message_ids: list[str], failed_ids: list[str]) -> tuple[list[str], bool]:
    failed_set = set(failed_ids)
    success_ids = [mid for mid in message_ids if mid not in failed_set]
    return success_ids, bool(failed_ids)


def test_partial_failed_calc() -> None:
    success_ids, partial_failed = split_success_and_failed(
        ["m1", "m2", "m3"],
        ["m2"],
    )
    assert success_ids == ["m1", "m3"]
    assert partial_failed is True


def test_all_success_calc() -> None:
    success_ids, partial_failed = split_success_and_failed(
        ["m1", "m2"],
        [],
    )
    assert success_ids == ["m1", "m2"]
    assert partial_failed is False
