"""Streamlit chat UI: `uv run streamlit run apps/streamlit_app.py`.

Runs the same core (filter, stop words, guard, agent, sanitizer) as the Slack bot. Config:
SILK_AGENT_CONFIG (default config/silk-agent.yaml, falls back to the example file). By default
it uses LocalProcessRuntime (no isolation; development only)."""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path

import streamlit as st

from silk_agent.app import Services, build_services
from silk_agent.channels.streamlit_channel import StreamlitChannel, build_view
from silk_agent.config import load_config
from silk_agent.core.events import UserMessage

ROOT = Path(__file__).resolve().parent.parent
POLL_SECONDS = 0.5


def _config():
    path = Path(os.environ.get("SILK_AGENT_CONFIG", ROOT / "config" / "silk-agent.yaml"))
    if not path.exists():
        path = ROOT / "config" / "silk-agent.example.yaml"
    config = load_config(path)
    config.filter.allowed_teams = []  # no Slack workspace here
    config.filter.allowed_channels = []
    config.filter.deny_guests = config.filter.deny_external_users = False  # Slack-only
    if os.environ.get("SILK_AGENT_SANDBOX", "local_process") == "local_process":
        config.sandbox.runtime = "local_process"
    config.skills.source = os.environ.get("SILK_AGENT_SKILLS") or None
    if os.environ.get("SILK_AGENT_MODEL"):
        config.agent.model = os.environ["SILK_AGENT_MODEL"]
    if os.environ.get("SILK_AGENT_GUARD") == "off":
        config.input_guard.provider = "allow_all"
        config.output_sanitizer.provider = "noop"
    return config


class _Backend:
    """The asyncio loop and services live in a background thread, shared by all reruns."""

    def __init__(self) -> None:
        self.channel = StreamlitChannel()
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        work = Path(os.environ.get("SILK_AGENT_WORKDIR") or tempfile.mkdtemp(prefix="silk-agent-"))
        self.services: Services = self.call(build_services(_config(), self.channel, work_dir=work))

    def call(self, coro, timeout: float = 120):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def submit(self, coro) -> None:
        asyncio.run_coroutine_threadsafe(coro, self.loop)


@st.cache_resource
def backend() -> _Backend:
    return _Backend()


def main() -> None:
    st.set_page_config(page_title="silk-agent", page_icon="🧵")
    st.title("silk-agent")
    be = backend()
    if "conversation" not in st.session_state:
        st.session_state.conversation = f"streamlit:{uuid.uuid4().hex}"
    key: str = st.session_state.conversation

    with st.sidebar:
        st.caption(f"会話: `{key}`")
        st.caption("実行中に「停止」と入力すると処理を止められます。")
        if st.button("新しい会話"):
            st.session_state.conversation = f"streamlit:{uuid.uuid4().hex}"
            st.rerun()

    for item in build_view(be.channel.log(key)):
        if item.kind == "user":
            with st.chat_message("user"):
                st.markdown(item.text)
        elif item.kind == "assistant":
            with st.chat_message("assistant"):
                st.markdown(item.text)
        elif item.kind == "tools":
            label = {
                "running": "ツール実行中",
                "complete": "ツール実行",
                "error": "ツール実行（失敗）",
                "interrupted": "ツール実行（中断）",
            }[item.state or "complete"]
            with st.chat_message("assistant"):
                with st.status(
                    f"{label}（{len(item.tools)}件）",
                    expanded=item.state == "running",
                    state="running" if item.state == "running" else "complete",
                ):
                    icons = {
                        "running": "⏳",
                        "success": "✅",
                        "failure": "❌",
                        "interrupted": "⏹",
                        "background": "🔄",
                    }
                    for t in item.tools:
                        extra = f"（{t.duration}）" if t.duration else ""
                        st.write(f"{icons[t.state]} `{t.name}` {t.summary}{extra}")
        elif item.kind == "notice":
            st.warning(item.text)
        else:
            st.caption(item.text)

    prompt = st.chat_input("依頼を入力（実行中は「停止」で中断）")
    if prompt:
        be.channel.add_user_message(key, prompt)
        be.submit(be.services.orchestrator.handle(UserMessage(key, "streamlit-user", prompt)))
        time.sleep(POLL_SECONDS)
        st.rerun()

    if be.channel.is_busy(key):
        time.sleep(POLL_SECONDS)
        st.rerun()


main()
