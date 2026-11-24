"""Compatibility for request methods missing from the pinned protocol."""

from e2a.message import ReqMethod

from agent_gateway.common.protocol_compat import ensure_protocol_member


def _ensure_req_method(name: str, value: str) -> None:
    ensure_protocol_member(ReqMethod, name, value)


# 企业版 Skill 包上传审批：协议包尚未发布这两个方法，先在此登记。
# 转发白名单、HTTP 路由、企业版门禁都从这里取值，避免同一方法名多处硬编码。
SKILL_UPLOAD_STAGE = "skills.upload.stage"
SKILL_UPLOAD_FINALIZE = "skills.upload.finalize"


# 企业版团队控制面：协议包尚未发布这 7 个方法，先在此登记（与 Agent 侧
# ``jiuwenswarm/common/schema/message.py`` 的 ``ensure_team_control_req_methods``
# 同名同值对齐）。企业版由启动流程登记后转发；个人版保持“未知方法”语义，
# 不允许通过 Web 入站的动态登记绕过 edition 门禁。
_TEAM_CONTROL_REQ_METHODS: tuple[tuple[str, str], ...] = (
    ("TEAM_CREATE", "team.create"),
    ("TEAM_GET", "team.get"),
    ("TEAM_STOP", "team.stop"),
    ("TEAM_MEMBER_JOIN", "team.member.join"),
    ("TEAM_MEMBER_LEAVE", "team.member.leave"),
    ("TEAM_MEMBERS_LIST", "team.members.list"),
    ("TEAM_QUOTA_GET", "team.quota.get"),
)


TEAM_CONTROL_METHODS = frozenset(value for _, value in _TEAM_CONTROL_REQ_METHODS)


def ensure_team_control_req_methods() -> None:
    """登记企业团队控制面方法；个人版不调用（避免暴露不可达的方法名）。"""
    for name, value in _TEAM_CONTROL_REQ_METHODS:
        _ensure_req_method(name, value)


def register_edition_req_methods() -> None:
    """按当前 edition 补齐协议包缺失的方法成员。

    必须在 dotenv 已加载之后调用（``JIUWENSWARM_EDITION`` 决定企业版语义），
    因此由启动流程（``apply_group_environment`` 之后）显式触发，不在 import 期执行。
    """
    from agent_gateway.enterprise.edition import is_enterprise

    if is_enterprise():
        ensure_team_control_req_methods()


def is_dynamic_req_method_denied(value: str) -> bool:
    """个人版禁止通过动态登记暴露企业版团队控制面方法。

    个人版已把这 7 个方法从转发白名单剔除；若允许 Web 入站动态登记，默认透传
    会绕过 edition 门禁，因此这里保持“未知方法”语义。
    """
    if value not in TEAM_CONTROL_METHODS:
        return False
    from agent_gateway.enterprise.edition import is_enterprise

    return not is_enterprise()


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
_ensure_req_method("SKILLS_UPLOAD_STAGE", SKILL_UPLOAD_STAGE)
_ensure_req_method("SKILLS_UPLOAD_FINALIZE", SKILL_UPLOAD_FINALIZE)
_ensure_req_method("WORKSPACE_TREE", "workspace.tree")
_ensure_req_method("WORKSPACE_PREVIEW", "workspace.preview")
_ensure_req_method("WORKSPACE_USAGE", "workspace.usage")
_ensure_req_method("WORKSPACE_ENTRIES_DELETE", "workspace.entries.delete")
_ensure_req_method("WORKSPACE_DOWNLOAD", "workspace.download")
_ensure_req_method("VIDEO_CONVERSATION_APPEND", "video.conversation.append")
# 企业技能广场 / 技能源 SPI：与 Agent 侧 message.py 对齐。缺失时 HTTP
# /skills/sources* 会在 Gateway 被映射成 skills.source.* 后直接 METHOD_NOT_FOUND。
_ensure_req_method("SKILLS_SOURCE_PROVIDERS", "skills.source.providers")
_ensure_req_method("SKILLS_SOURCE_SEARCH", "skills.source.search")
_ensure_req_method("SKILLS_SOURCE_INSTALL", "skills.source.install")
_ensure_req_method("SKILLS_UPDATES_CHECK", "skills.updates.check")
_ensure_req_method("SKILLS_UPDATE", "skills.update")
