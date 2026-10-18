from silk_agent.channels.slack_events import SlackEventRouter, make_key, parse_key, slack_unescape

BOT = "UBOT"


def body(event, event_id="Ev1", team="T1"):
    return {"team_id": team, "event_id": event_id, "event": event}


class Env:
    def __init__(self, participating=()):
        self.handled = []
        self.participating = set(participating)
        self.marked = []

        async def handle(e):
            self.handled.append(e)

        async def is_part(key):
            return key in self.participating

        self.router = SlackEventRouter(
            handle, bot_user_id=BOT, is_participating=is_part, mark_participating=self.marked.append
        )


def test_key_roundtrip():
    assert make_key("T", "C", "1.5") == "slack:T:C:1.5"
    assert parse_key("slack:T:C:1.5") == ("T", "C", "1.5")
    assert parse_key(make_key("T", "D", None)) == ("T", "D", None)


async def test_mention_starts_conversation_in_message_thread():
    env = Env()
    await env.router.on_app_mention(
        body(
            {
                "type": "app_mention",
                "user": "U1",
                "text": "<@UBOT> テスト実行して",
                "channel": "C1",
                "ts": "100.1",
            }
        )
    )
    (m,) = env.handled
    assert m.conversation_key == "slack:T1:C1:100.1"
    assert m.text == "テスト実行して" and m.user == "U1" and m.team == "T1" and m.channel == "C1"
    assert m.kind == "message" and not m.sender_is_bot
    assert env.marked == ["slack:T1:C1:100.1"]


async def test_mention_inside_existing_thread_uses_that_thread():
    env = Env()
    await env.router.on_app_mention(
        body(
            {
                "user": "U1",
                "text": "<@UBOT> hi",
                "channel": "C1",
                "ts": "105.0",
                "thread_ts": "100.1",
            }
        )
    )
    assert env.handled[0].conversation_key == "slack:T1:C1:100.1"


async def test_reply_in_participating_thread_is_handled_as_same_conversation():
    env = Env(participating={"slack:T1:C1:100.1"})
    await env.router.on_message(
        body(
            {
                "user": "U1",
                "text": "続けて",
                "channel": "C1",
                "channel_type": "channel",
                "ts": "101.0",
                "thread_ts": "100.1",
            }
        )
    )
    assert env.handled[0].conversation_key == "slack:T1:C1:100.1"
    assert env.handled[0].text == "続けて"


async def test_reply_in_thread_bot_never_joined_is_ignored():
    env = Env()
    await env.router.on_message(
        body(
            {
                "user": "U1",
                "text": "hello",
                "channel": "C1",
                "channel_type": "channel",
                "ts": "101.0",
                "thread_ts": "100.1",
            }
        )
    )
    assert env.handled == []


async def test_top_level_channel_message_without_mention_is_ignored():
    env = Env(participating={"slack:T1:C1:100.1"})
    await env.router.on_message(
        body(
            {"user": "U1", "text": "hi", "channel": "C1", "channel_type": "channel", "ts": "100.1"}
        )
    )
    assert env.handled == []


async def test_dm_is_handled():
    env = Env()
    await env.router.on_message(
        body({"user": "U1", "text": "hi", "channel": "D1", "channel_type": "im", "ts": "1.0"})
    )
    assert env.handled[0].conversation_key == "slack:T1:D1:-"
    await env.router.on_message(
        body(
            {
                "user": "U1",
                "text": "in thread",
                "channel": "D1",
                "channel_type": "im",
                "ts": "2.0",
                "thread_ts": "1.0",
            },
            event_id="Ev2",
        )
    )
    assert env.handled[1].conversation_key == "slack:T1:D1:1.0"


async def test_own_posts_are_ignored_and_other_bots_are_flagged_for_the_filter():
    env = Env(participating={"slack:T1:C1:100.1"})
    thread = {"channel": "C1", "channel_type": "channel", "ts": "102.0", "thread_ts": "100.1"}
    await env.router.on_message(body({"user": BOT, "text": "progress", **thread}, "Ev1"))
    assert env.handled == []
    await env.router.on_message(
        body(
            {"bot_id": "B9", "user": "UOTHER", "text": "I am a bot", **thread, "ts": "103.0"}, "Ev2"
        )
    )
    assert env.handled[0].sender_is_bot is True


async def test_system_events_have_non_message_kind():
    env = Env(participating={"slack:T1:C1:100.1"})
    await env.router.on_message(
        body(
            {
                "user": "U1",
                "subtype": "message_changed",
                "channel": "C1",
                "channel_type": "channel",
                "ts": "104.0",
                "thread_ts": "100.1",
            }
        )
    )
    assert env.handled[0].kind == "message_changed"


async def test_redelivered_event_is_processed_once():
    env = Env()
    ev = {"user": "U1", "text": "<@UBOT> go", "channel": "C1", "ts": "100.1"}
    await env.router.on_app_mention(body(ev, "EvSame"))
    await env.router.on_app_mention(body(ev, "EvSame"))
    assert len(env.handled) == 1


async def test_same_post_as_mention_and_message_is_processed_once():
    env = Env(participating={"slack:T1:C1:100.1"})
    ev = {
        "user": "U1",
        "text": "<@UBOT> next",
        "channel": "C1",
        "channel_type": "channel",
        "ts": "106.0",
        "thread_ts": "100.1",
    }
    await env.router.on_app_mention(body(ev, "EvA"))
    await env.router.on_message(body(ev, "EvB"))
    assert len(env.handled) == 1


async def test_attachments_are_listed_by_name():
    env = Env()
    await env.router.on_app_mention(
        body(
            {
                "user": "U1",
                "text": "<@UBOT> see",
                "channel": "C1",
                "ts": "1.0",
                "files": [{"name": "a.log"}],
            }
        )
    )
    assert env.handled[0].attachments == ("a.log",)


def test_slack_escapes_are_undone():
    assert slack_unescape("sleep 300 &amp;&amp; echo done") == "sleep 300 && echo done"
    assert slack_unescape("a &lt;b&gt; c") == "a <b> c"
    assert slack_unescape("&amp;lt;") == "&lt;"  # a literal "&lt;" typed by the user


# ---------------------------------------------------------------- sender resolution


class FakeUsersClient:
    def __init__(self, users=None, fail=()):
        self.users = users or {}
        self.fail = set(fail)
        self.calls = []

    async def users_info(self, user):
        self.calls.append(user)
        if user in self.fail:
            raise RuntimeError("ratelimited")
        return {"user": self.users[user]}


def slack_user(email="", *, restricted=False, ultra=False, team="T1", enterprise=""):
    user = {
        "profile": {"email": email},
        "is_restricted": restricted,
        "is_ultra_restricted": ultra,
        "team_id": team,
    }
    if enterprise:
        user["enterprise_user"] = {"enterprise_id": enterprise}
    return user


def sender_env(users, *, team="T1", enterprise="", clock=None):
    from silk_agent.channels.slack_users import UserDirectory

    env = Env()
    client = FakeUsersClient(**users)
    env.client = client
    handled = env.handled

    async def handle(e):
        handled.append(e)

    async def is_part(key):
        return False

    env.router = SlackEventRouter(
        handle,
        bot_user_id=BOT,
        is_participating=is_part,
        users=UserDirectory(client, clock),
        workspace_team=team,
        workspace_enterprise=enterprise,
    )
    return env


def mention(user="U1", **extra):
    return body({"user": user, "text": "<@UBOT> hi", "channel": "C1", "ts": "100.1", **extra})


async def test_sender_info_has_the_email_and_is_internal_by_default():
    env = sender_env({"users": {"U1": slack_user("a@example.com")}})
    await env.router.on_app_mention(mention())
    (m,) = env.handled
    assert m.sender is not None
    assert (m.sender.email, m.sender.is_guest, m.sender.is_external, m.sender.team) == (
        "a@example.com",
        False,
        False,
        "T1",
    )
    assert not m.is_direct


async def test_guests_are_detected():
    users = {
        "U1": slack_user("m@example.com", restricted=True),
        "U2": slack_user("s@example.com", ultra=True),
    }
    env = sender_env({"users": users})
    await env.router.on_app_mention(mention("U1"))
    await env.router.on_app_mention(body({**mention("U2")["event"], "ts": "101.1"}, "Ev2"))
    assert [m.sender.is_guest for m in env.handled] == [True, True]


async def test_slack_connect_user_is_external_by_user_team():
    env = sender_env({"users": {"U9": slack_user("x@other.example", team="T9")}})
    await env.router.on_app_mention(mention("U9", user_team="T9"))
    assert env.handled[0].sender.is_external is True


async def test_enterprise_grid_user_of_another_workspace_in_the_same_org_is_internal():
    env = sender_env(
        {"users": {"U2": slack_user("o@example.com", team="T2", enterprise="E1")}},
        enterprise="E1",
    )
    await env.router.on_app_mention(mention("U2", user_team="T2"))
    assert env.handled[0].sender.is_external is False


async def test_enterprise_grid_user_of_another_org_is_external():
    env = sender_env(
        {"users": {"U2": slack_user("o@other.example", team="T2", enterprise="E2")}},
        enterprise="E1",
    )
    await env.router.on_app_mention(mention("U2", user_team="T2"))
    assert env.handled[0].sender.is_external is True


async def test_failed_lookup_leaves_guest_unknown_and_is_not_cached():
    env = sender_env({"users": {"U1": slack_user("a@example.com")}, "fail": ["U1"]})
    await env.router.on_app_mention(mention())
    s = env.handled[0].sender
    assert s is not None and s.is_guest is None and s.email == ""
    # same workspace by the event's own team field -> known
    await env.router.on_app_mention(body({**mention(user_team="T1")["event"], "ts": "2.1"}, "Ev2"))
    assert env.handled[1].sender.is_external is False
    env.client.fail.clear()
    await env.router.on_app_mention(body({**mention()["event"], "ts": "3.1"}, "Ev3"))
    assert env.handled[2].sender.is_guest is False  # recovered: the failure was not cached


async def test_failed_lookup_in_grid_makes_external_unknown():
    env = sender_env({"users": {}, "fail": ["U2"]}, enterprise="E1")
    await env.router.on_app_mention(mention("U2", user_team="T2"))
    assert env.handled[0].sender.is_external is None


async def test_lookups_are_cached_for_ten_minutes():
    from silk_agent.testing.fakes import FakeClock

    clock = FakeClock()
    env = sender_env({"users": {"U1": slack_user("a@example.com")}}, clock=clock)
    for i in range(3):
        await env.router.on_app_mention(body({**mention()["event"], "ts": f"{i}.1"}, f"Ev{i}"))
    assert env.client.calls == ["U1"]
    await clock.advance(601)
    await env.router.on_app_mention(body({**mention()["event"], "ts": "9.1"}, "Ev9"))
    assert env.client.calls == ["U1", "U1"]


async def test_dm_is_marked_direct():
    env = sender_env({"users": {"U1": slack_user("a@example.com")}})
    await env.router.on_message(
        body({"user": "U1", "text": "hi", "channel": "D1", "channel_type": "im", "ts": "1.1"})
    )
    assert env.handled[0].is_direct is True


async def test_the_arrival_is_taken_before_the_slow_sender_lookup():
    import asyncio

    handled, reserved, released = [], [], []
    gate = asyncio.Event()

    class SlowUsers:
        async def lookup(self, user):
            if user == "SLOW":
                await gate.wait()
            return None

    async def handle(event):
        handled.append(event)

    async def is_part(key):
        return False

    router = SlackEventRouter(
        handle,
        bot_user_id=BOT,
        is_participating=is_part,
        users=SlowUsers(),
        reserve=lambda key: reserved.append(key) or len(reserved),
        release=lambda key, arrival: released.append(arrival),
    )
    slow = asyncio.create_task(
        router.on_message(
            body(
                {"user": "SLOW", "text": "A", "channel": "D1", "channel_type": "im", "ts": "1.1"},
                "EvA",
            )
        )
    )
    await asyncio.sleep(0)
    await router.on_message(
        body({"user": "U2", "text": "B", "channel": "D1", "channel_type": "im", "ts": "2.2"}, "EvB")
    )
    gate.set()
    await slow
    # B was handled first, but A holds the earlier place in the arrival order.
    assert [(m.text, m.arrival) for m in handled] == [("B", 2), ("A", 1)]
    assert sorted(released) == [1, 2]
