"""Compatibility for request methods missing from the pinned protocol."""

from enum import Enum

from e2a.message import ReqMethod


def _ensure_protocol_member(enum_cls: type[Enum], name: str, value: str) -> None:
    """Register a member of a protocol enum the pinned protocol package does not ship yet.

    ``e2a`` owns the enum; 这里只为协议包尚未发布的成员开一个缺口，
    待协议包提供同名同值成员后本函数与调用一并删除。
    """
    existing = enum_cls.__members__.get(name)
    if existing is not None:
        if existing.value != value:
            raise RuntimeError(
                f"{enum_cls.__name__}.{name} is {existing.value!r}, expected {value!r}"
            )
        return
    if any(item.value == value for item in enum_cls):
        raise RuntimeError(f"{enum_cls.__name__} value {value!r} is already registered")
    # Enum exposes no public API for adding a member, so extending the pinned
    # protocol enum requires its internal registries. Keep those accesses here.
    member = object.__new__(enum_cls)
    member._name_ = name  # pylint: disable=protected-access
    member._value_ = value  # pylint: disable=protected-access
    type.__setattr__(enum_cls, name, member)
    enum_cls._member_map_[name] = member  # pylint: disable=protected-access
    enum_cls._value2member_map_[value] = member  # pylint: disable=protected-access
    enum_cls._member_names_.append(name)  # pylint: disable=protected-access


def _ensure_req_method(name: str, value: str) -> None:
    _ensure_protocol_member(ReqMethod, name, value)


_ensure_req_method("RECOMMENDATION", "recommendation.job")
_ensure_req_method("TRAJECTORY_SUBJECTS_LIST", "trajectory.subjects.list")
_ensure_req_method("TRAJECTORY_STREAM_FRAMES_GET", "trajectory.stream_frames.get")
_ensure_req_method("TRAJECTORY_SEQUENCES_GET", "trajectory.sequences.get")
_ensure_req_method("TRAJECTORY_CHECKPOINTS_GET", "trajectory.checkpoints.get")
_ensure_req_method("TRAJECTORY_USAGE_GET", "trajectory.usage.get")
_ensure_req_method("TRAJECTORY_SUBJECT_GET", "trajectory.subject.get")
_ensure_req_method("TRAJECTORY_RAW_GET", "trajectory.raw.get")
_ensure_req_method("TRAJECTORY_ARCHIVE_OPEN", "trajectory.archive.open")
_ensure_req_method("TRAJECTORY_BLOB_READ", "trajectory.blob.read")
_ensure_req_method("TRAJECTORY_BLOB_CLOSE", "trajectory.blob.close")
_ensure_req_method("FILE_READ_UPLOAD_CHUNK", "file.read_upload_chunk")
_ensure_req_method("WORKSPACE_TREE", "workspace.tree")
_ensure_req_method("WORKSPACE_PREVIEW", "workspace.preview")
_ensure_req_method("WORKSPACE_USAGE", "workspace.usage")
_ensure_req_method("WORKSPACE_ENTRIES_DELETE", "workspace.entries.delete")
_ensure_req_method("VIDEO_CONVERSATION_APPEND", "video.conversation.append")
