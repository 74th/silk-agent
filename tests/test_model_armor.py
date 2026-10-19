from types import SimpleNamespace

from google.cloud import modelarmor_v1 as ma

from silk_agent.guards.model_armor import ModelArmorSanitizer, split_text

MATCH = ma.FilterMatchState.MATCH_FOUND
NO_MATCH = ma.FilterMatchState.NO_MATCH_FOUND


def result(invocation=ma.InvocationResult.SUCCESS, **filters):
    """Build a SanitizationResult from {name: FilterResult} (the API reports SUCCESS)."""
    state = MATCH if any(_matched(f) for f in filters.values()) else NO_MATCH
    return ma.SanitizationResult(
        filter_match_state=state, filter_results=filters, invocation_result=invocation
    )


def _matched(fr):
    pb = type(fr).pb(fr)
    kind = pb.WhichOneof("filter_result")
    sub = getattr(fr, kind)
    if kind == "sdp_filter_result":
        k = type(sub).pb(sub).WhichOneof("result")
        return k is not None and getattr(sub, k).match_state == MATCH
    return sub.match_state == MATCH


def sdp(masked: str | None = "MASKED", state=MATCH):
    if masked is None:
        inner = ma.SdpFilterResult(inspect_result=ma.SdpInspectResult(match_state=state))
    else:
        inner = ma.SdpFilterResult(
            deidentify_result=ma.SdpDeidentifyResult(
                match_state=state, data=ma.DataItem(text=masked)
            )
        )
    return ma.FilterResult(sdp_filter_result=inner)


def uri(state=MATCH):
    return ma.FilterResult(
        malicious_uri_filter_result=ma.MaliciousUriFilterResult(match_state=state)
    )


def pi(state=MATCH):
    return ma.FilterResult(
        pi_and_jailbreak_filter_result=ma.PiAndJailbreakFilterResult(match_state=state)
    )


class FakeClient:
    def __init__(self, results=None, error=None):
        self.results = list(results or [])
        self.error = error
        self.requests = []

    async def sanitize_model_response(self, request):
        self.requests.append(request)
        if self.error:
            raise self.error
        return SimpleNamespace(sanitization_result=self.results.pop(0))


def make(client, **kw):
    return ModelArmorSanitizer(
        template="projects/p/locations/l/templates/simple", client=client, **kw
    )


async def test_no_match_passes_text_through():
    c = FakeClient([result(sdp=sdp(state=NO_MATCH), pi=pi(NO_MATCH))])
    r = await make(c).sanitize("テストは3件すべて成功しました")
    assert (r.status, r.text) == ("pass", "テストは3件すべて成功しました")
    assert c.requests[0].name.endswith("/templates/simple")
    assert c.requests[0].model_response_data.text == "テストは3件すべて成功しました"


async def test_sdp_only_is_masked():
    c = FakeClient([result(sdp=sdp("連絡先は [EMAIL_ADDRESS] です"))])
    r = await make(c).sanitize("連絡先は a@example.com です")
    assert (r.status, r.text) == ("masked", "連絡先は [EMAIL_ADDRESS] です")


async def test_sdp_without_masked_text_is_blocked():
    c = FakeClient([result(sdp=sdp(None))])
    r = await make(c).sanitize("a@example.com")
    assert r.status == "blocked" and r.text == ""


async def test_sdp_empty_masked_text_is_blocked():
    c = FakeClient([result(sdp=sdp(""))])
    assert (await make(c).sanitize("a@example.com")).status == "blocked"


async def test_other_filter_blocks_even_with_sdp():
    c = FakeClient([result(sdp=sdp("x"), uri=uri())])
    r = await make(c).sanitize("http://bad.example a@example.com")
    assert r.status == "blocked" and "malicious_uri_filter_result" in r.reason


async def test_jailbreak_match_is_ignored_by_default():
    # Model Armor flags ordinary shell commands; bulk_files.py handles jailbreaks instead.
    c = FakeClient([result(pi_and_jailbreak=pi())])
    r = await make(c).sanitize("echo hello > a.txt && whoami")
    assert (r.status, r.text) == ("pass", "echo hello > a.txt && whoami")


async def test_jailbreak_with_sdp_is_masked():
    c = FakeClient([result(pi_and_jailbreak=pi(), sdp=sdp("[EMAIL_ADDRESS]"))])
    r = await make(c).sanitize("a@example.com")
    assert (r.status, r.text) == ("masked", "[EMAIL_ADDRESS]")


async def test_jailbreak_blocks_when_not_ignored():
    c = FakeClient([result(pi_and_jailbreak=pi())])
    assert (await make(c, ignored_filters=()).sanitize("x")).status == "blocked"


async def test_on_sdp_match_block():
    c = FakeClient([result(sdp=sdp("x"))])
    assert (await make(c, on_sdp_match="block").sanitize("a@example.com")).status == "blocked"


async def test_failure_is_error_not_pass():
    c = FakeClient(error=RuntimeError("503"))
    r = await make(c).sanitize("hello")
    assert r.status == "error" and r.text == ""


async def test_empty_text_needs_no_call():
    c = FakeClient()
    assert (await make(c).sanitize("  \n")).status == "pass"
    assert not c.requests


async def test_long_text_is_split_and_every_part_checked():
    text = "\n\n".join(f"paragraph {i} " + "x" * 90 for i in range(10))
    parts = split_text(text, 300)
    assert len(parts) > 1 and "".join(parts) == text
    c = FakeClient([result(sdp=sdp(state=NO_MATCH)) for _ in parts])
    r = await make(c, chunk_chars=300).sanitize(text)
    assert r.status == "pass" and r.text == text
    assert [q.model_response_data.text for q in c.requests] == parts


async def test_one_blocked_part_blocks_everything():
    text = "a" * 250 + "\n\n" + "b" * 250
    c = FakeClient([result(sdp=sdp(state=NO_MATCH)), result(uri=uri())])
    r = await make(c, chunk_chars=300).sanitize(text)
    assert r.status == "blocked" and r.text == ""


async def test_masked_parts_are_joined():
    text = "a" * 250 + "\n\n" + "b" * 250
    c = FakeClient([result(sdp=sdp("A")), result(sdp=sdp(state=NO_MATCH))])
    r = await make(c, chunk_chars=300).sanitize(text)
    assert r.status == "masked" and r.text == "A" + "b" * 250


def test_split_keeps_code_blocks_together():
    code = "```\n" + "\n\n".join(f"line{i}" for i in range(5)) + "\n```\n\n"
    text = "intro\n\n" + code + "outro\n"
    parts = split_text(text, len(code) + 5)
    assert "".join(parts) == text
    assert any(code in p for p in parts)


def test_split_oversized_single_line():
    text = "x" * 1000
    parts = split_text(text, 300)
    assert "".join(parts) == text and all(len(p) <= 300 for p in parts)


def test_no_match_from_an_incomplete_invocation_is_not_a_pass():
    # Some filters may not have run: "nothing matched" proves nothing.
    for invocation in (
        ma.InvocationResult.PARTIAL,
        ma.InvocationResult.FAILURE,
        ma.InvocationResult.INVOCATION_RESULT_UNSPECIFIED,
    ):
        out = ModelArmorSanitizer.interpret(
            result(invocation, pi_and_jailbreak=pi(NO_MATCH)), "text"
        )
        assert out.status == "error", invocation
        assert out.text == ""


def test_an_incomplete_invocation_is_never_shown_whatever_matched():
    # An ignored filter matching, an SDP match (masked text), a harmful match: none of them
    # makes the text safe to show when other filters did not run.
    ignored = ModelArmorSanitizer.interpret(
        result(ma.InvocationResult.PARTIAL, pi_and_jailbreak=pi()),
        "contact private@example.com",
        ignored_filters={"pi_and_jailbreak"},
    )
    masked = ModelArmorSanitizer.interpret(
        result(ma.InvocationResult.PARTIAL, sdp=sdp("MASKED")), "secret"
    )
    harmful = ModelArmorSanitizer.interpret(
        result(ma.InvocationResult.FAILURE, malicious_uris=uri()), "http://bad.example"
    )
    for out in (ignored, masked, harmful):
        assert out.status == "error" and out.text == ""


async def test_sanitize_returns_the_error_of_a_partial_invocation_not_an_empty_pass():
    # interpret() says "error"; sanitize() used to join that chunk as empty text and answer
    # "pass", so the user got a blank reply and no notice that the check had failed.
    c = FakeClient([result(ma.InvocationResult.PARTIAL, pi_and_jailbreak=pi())])
    r = await make(c, ignored_filters={"pi_and_jailbreak"}).sanitize("contact private@example.com")
    assert r.status == "error" and r.text == ""


async def test_one_unchecked_chunk_makes_the_whole_output_unshowable():
    first = result(sdp=sdp(state=NO_MATCH), pi=pi(NO_MATCH))
    second = result(ma.InvocationResult.FAILURE, pi=pi(NO_MATCH))
    c = FakeClient([first, second])
    r = await make(c, chunk_chars=5).sanitize("aaaaa" + "bbbbb")
    assert len(c.requests) == 2
    assert r.status == "error" and r.text == ""  # not "aaaaa" shown with a hole after it
