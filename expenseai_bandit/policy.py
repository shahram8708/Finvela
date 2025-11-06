"""Helpers for retrieving and updating contextual bandit policies."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from expenseai_ext.db import db
from expenseai_models.bandit_policy import BanditPolicy


def get_active_policy() -> BanditPolicy | None:
    """Return the current active policy, falling back to the latest seed."""
    stmt = select(BanditPolicy).where(BanditPolicy.is_active.is_(True)).order_by(BanditPolicy.updated_at.desc())
    policy = db.session.execute(stmt).scalars().first()
    if policy is not None:
        return policy
    stmt = select(BanditPolicy).order_by(BanditPolicy.updated_at.desc())
    return db.session.execute(stmt).scalars().first()


def activate_policy(policy: BanditPolicy) -> None:
    """Mark the provided policy as active and deactivate previous ones."""
    db.session.query(BanditPolicy).update({BanditPolicy.is_active: False})
    policy.is_active = True
    policy.updated_at = datetime.utcnow()
    db.session.flush()


def create_policy(version: str, weights: dict[str, float], alpha: float) -> BanditPolicy:
    """Persist a new policy version with weights and alpha."""
    policy = BanditPolicy(version=version, weights_json=weights, alpha=alpha, is_active=False)
    db.session.add(policy)
    db.session.flush()
    return policy
