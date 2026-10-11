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
