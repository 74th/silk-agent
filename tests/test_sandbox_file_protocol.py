import pytest
from pydantic import ValidationError

from silk_agent.sandbox.protocol import FileListRequest, FileReadRequest, FileWriteRequest

IDENTITY = dict(
    request_id="r",
    conversation_key="c",
    owner_epoch=1,
    conversation_generation=1,
    turn_id="t",
    origin_seq=1,
)


@pytest.mark.parametrize("request_type", [FileListRequest, FileReadRequest, FileWriteRequest])
@pytest.mark.parametrize(
    "path", ["/workspace/file", "../file", "a/../../file", ".session-runtime/state", "a\x00b", ""]
)
def test_invalid_file_paths_are_rejected_before_manager_or_repository_access(request_type, path):
    extra = {"data_base64": ""} if request_type is FileWriteRequest else {}
    with pytest.raises(ValidationError):
        request_type(**IDENTITY, path=path, **extra)


def test_relative_skill_paths_and_workspace_listing_remain_available():
    assert FileReadRequest(**IDENTITY, path=".skills/skill.md").path == ".skills/skill.md"
    assert FileListRequest(**IDENTITY).path == "."
