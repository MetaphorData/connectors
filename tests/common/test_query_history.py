from metaphor.common.query_history import user_id_or_email


def test_user_id_or_email():
    user_id, email = user_id_or_email("not_an_email")
    assert user_id == "not_an_email"
    assert email is None

    user_id, email = user_id_or_email("test@test.com")
    assert user_id is None
    assert email == "test@test.com"
