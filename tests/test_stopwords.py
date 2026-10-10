import pytest

from silk_agent.core.stopwords import StopWords

SW = StopWords(["停止", "中止", "中断", "STOP"])


@pytest.mark.parametrize(
    "text", ["停止", "中止", "中断", "STOP", "stop", " ｓｔｏｐ ", "Ｓｔｏｐ\n"]
)
def test_matches(text):
    assert SW.matches(text)


@pytest.mark.parametrize("text", ["処理を停止して別のことをして", "stop it", "停止!", ""])
def test_does_not_match(text):
    assert not SW.matches(text)


def test_custom_words():
    assert StopWords(["やめて"]).matches(" やめて ")
