from datetime import UTC, datetime

from silk_agent.config import Config, FilterConfig
from silk_agent.distributed.control_policy import build_control_authorizer


class Clock:
    def now(self):
        return datetime(2026, 10, 15, tzinfo=UTC)


def record(**sender):
    return {
        "conversation_key": "slack:T:C:1",
        "payload": {
            "text": "STOP",
            "user": "U1",
            "team": "T",
            "channel": "C",
            "sender": {"email": "a@example.com", "is_guest": False, "is_external": False, **sender},
        },
    }


def test_default_policy_authorizes_a_resolved_regular_sender():
    assert build_control_authorizer(Config(), Clock())(record())


def test_policy_denies_guests_unlisted_users_and_missing_sender_info():
    config = Config(filter=FilterConfig(deny_guests=True, allowed_users=["U1"]))
    authorize = build_control_authorizer(config, Clock())
    assert authorize(record())
    assert not authorize(record(is_guest=True))
    assert not authorize({**record(), "payload": {**record()["payload"], "user": "U2"}})
    assert not authorize({"payload": {"text": "STOP", "user": "U1"}})
    assert not authorize({})


def test_unclassified_external_sender_is_denied_when_external_users_are_denied():
    config = Config(filter=FilterConfig(deny_external_users=True))
    assert not build_control_authorizer(config, Clock())(record(is_external=None))
