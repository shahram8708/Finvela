"""Views powering the organization chat experience."""
from __future__ import annotations

from datetime import datetime

from flask import abort, jsonify, render_template, request
from flask_login import current_user, login_required
from sqlalchemy import and_, or_

from expenseai_chat import chat_bp
from expenseai_chat.models import ChatMessage, GroupMessage
from expenseai_ext.db import db
from expenseai_models.user import User

_MAX_MESSAGE_LENGTH = 2000
_DEFAULT_HISTORY_LIMIT = 100


def _ensure_membership() -> tuple[int, int]:
    """Ensure the current user is attached to an organization."""
    if not current_user.is_authenticated:  # pragma: no cover - guarded by @login_required
        abort(401)
    if not current_user.organization_id:
        abort(403, description="You must join an organization before chatting.")
    return current_user.id, current_user.organization_id


def _resolve_teammate(user_id: int, organization_id: int) -> User:
    teammate = (
        User.query.filter(
            User.id == user_id,
            User.organization_id == organization_id,
        )
        .limit(1)
        .first()
    )
    if teammate is None:
        abort(404, description="Team member not found.")
    return teammate


@chat_bp.route("/chat/")
@login_required
def member_list() -> str:
    organization = current_user.organization
    if not organization:
        return render_template(
            "chat/member_list.html",
            members=[],
            organization=None,
        )

    members = (
        User.query.filter(
            User.organization_id == organization.id,
            User.id != current_user.id,
        )
        .order_by(User.full_name.asc())
        .all()
    )
    return render_template(
        "chat/member_list.html",
        members=members,
        organization=organization,
    )


@chat_bp.route("/chat/<int:user_id>/")
@login_required
def direct_chat(user_id: int) -> str:
    current_id, organization_id = _ensure_membership()
    teammate = _resolve_teammate(user_id, organization_id)
    if teammate.id == current_id:
        abort(400, description="Cannot start a chat with yourself.")

    messages = (
        ChatMessage.query.filter(
            ChatMessage.organization_id == organization_id,
            or_(
                and_(
                    ChatMessage.sender_id == current_id,
                    ChatMessage.receiver_id == teammate.id,
                ),
                and_(
                    ChatMessage.sender_id == teammate.id,
                    ChatMessage.receiver_id == current_id,
                ),
            ),
        )
        .order_by(ChatMessage.created_at.asc())
        .limit(_DEFAULT_HISTORY_LIMIT)
        .all()
    )

    return render_template(
        "chat/direct_chat.html",
        teammate=teammate,
        organization=current_user.organization,
        initial_messages=[message.as_dict(current_id) for message in messages],
    )


@chat_bp.route("/group_chat/")
@login_required
def group_chat() -> str:
    _, organization_id = _ensure_membership()

    messages = (
        GroupMessage.query.filter(GroupMessage.organization_id == organization_id)
        .order_by(GroupMessage.created_at.asc())
        .limit(_DEFAULT_HISTORY_LIMIT)
        .all()
    )

    return render_template(
        "chat/group_chat.html",
        organization=current_user.organization,
        initial_messages=[message.as_dict(current_user.id) for message in messages],
    )


@chat_bp.route("/get_messages/<int:receiver_id>/")
@login_required
def get_messages(receiver_id: int):
    current_id, organization_id = _ensure_membership()
    teammate = _resolve_teammate(receiver_id, organization_id)
    if teammate.id == current_id:
        return jsonify({"status": "ok", "messages": []})

    after_id = request.args.get("after_id", type=int)
    after_ts = request.args.get("after", type=float)

    query = ChatMessage.query.filter(
        ChatMessage.organization_id == organization_id,
        or_(
            and_(
                ChatMessage.sender_id == current_id,
                ChatMessage.receiver_id == teammate.id,
            ),
            and_(
                ChatMessage.sender_id == teammate.id,
                ChatMessage.receiver_id == current_id,
            ),
        ),
    )
    if after_id:
        query = query.filter(ChatMessage.id > after_id)
    if after_ts:
        try:
            threshold = datetime.utcfromtimestamp(after_ts)
        except (OverflowError, OSError, ValueError):
            threshold = None
        if threshold is not None:
            query = query.filter(ChatMessage.created_at > threshold)

    messages = query.order_by(ChatMessage.created_at.asc()).limit(200).all()
    payload = [message.as_dict(current_id) for message in messages]
    return jsonify({"status": "ok", "messages": payload})


@chat_bp.route("/send_message/", methods=["POST"])
@login_required
def send_message():
    current_id, organization_id = _ensure_membership()
    data = request.get_json(silent=True) or {}
    receiver_id = data.get("receiver_id")
    content = (data.get("message") or "").strip()

    if not receiver_id:
        return jsonify({"status": "error", "message": "Receiver is required."}), 400
    if not content:
        return jsonify({"status": "error", "message": "Message cannot be empty."}), 400
    if len(content) > _MAX_MESSAGE_LENGTH:
        return (
            jsonify({"status": "error", "message": "Message exceeds the allowed length."}),
            400,
        )

    try:
        target_id = int(receiver_id)
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Invalid receiver."}), 400

    if target_id <= 0:
        return jsonify({"status": "error", "message": "Invalid receiver."}), 400

    teammate = _resolve_teammate(target_id, organization_id)
    if teammate.id == current_id:
        return jsonify({"status": "error", "message": "Cannot send messages to yourself."}), 400

    message = ChatMessage(
        organization_id=organization_id,
        sender_id=current_id,
        receiver_id=teammate.id,
        message=content,
    )
    db.session.add(message)
    db.session.commit()

    return jsonify({"status": "ok", "message": message.as_dict(current_id)})


@chat_bp.route("/get_group_messages/")
@login_required
def get_group_messages():
    current_id, organization_id = _ensure_membership()

    after_id = request.args.get("after_id", type=int)
    after_ts = request.args.get("after", type=float)

    query = GroupMessage.query.filter(GroupMessage.organization_id == organization_id)
    if after_id:
        query = query.filter(GroupMessage.id > after_id)
    if after_ts:
        try:
            threshold = datetime.utcfromtimestamp(after_ts)
        except (OverflowError, OSError, ValueError):
            threshold = None
        if threshold is not None:
            query = query.filter(GroupMessage.created_at > threshold)

    messages = query.order_by(GroupMessage.created_at.asc()).limit(200).all()
    payload = [message.as_dict(current_id) for message in messages]
    return jsonify({"status": "ok", "messages": payload})


@chat_bp.route("/send_group_message/", methods=["POST"])
@login_required
def send_group_message():
    current_id, organization_id = _ensure_membership()
    data = request.get_json(silent=True) or {}
    content = (data.get("message") or "").strip()

    if not content:
        return jsonify({"status": "error", "message": "Message cannot be empty."}), 400
    if len(content) > _MAX_MESSAGE_LENGTH:
        return (
            jsonify({"status": "error", "message": "Message exceeds the allowed length."}),
            400,
        )

    message = GroupMessage(
        organization_id=organization_id,
        sender_id=current_id,
        message=content,
    )
    message.sender = current_user
    db.session.add(message)
    db.session.commit()

    return jsonify({"status": "ok", "message": message.as_dict(current_id)})
