"""Billing helpers responsible for Razorpay-backed organization upgrades."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from flask import current_app

from expenseai_auth.services import OrganizationService
from expenseai_ext.db import db
from expenseai_models.organization import Organization
from expenseai_models.organization_subscription import OrganizationSubscription
from expenseai_models.user import User


class BillingError(Exception):
    """Base exception for billing workflow errors."""


class BillingConfigurationError(BillingError):
    """Raised when Razorpay credentials or pricing knobs are missing."""


class PaymentVerificationError(BillingError):
    """Raised when Razorpay payment verification fails."""


@dataclass(frozen=True)
class PricingBreakdown:
    """Represents the computed price for an organization upgrade request."""

    currency: str
    current_limit: int
    desired_limit: int
    additional_users: int
    per_user_price_minor: int
    total_amount_minor: int

    @property
    def has_change(self) -> bool:
        return self.additional_users > 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "currency": self.currency,
            "current_limit": self.current_limit,
            "desired_limit": self.desired_limit,
            "additional_users": self.additional_users,
            "per_user_price_minor": self.per_user_price_minor,
            "total_amount_minor": self.total_amount_minor,
        }


class OrganizationBillingService:
    """Service object encapsulating Razorpay integration for seat upgrades."""

    @staticmethod
    def is_configured() -> bool:
        config = current_app.config
        return bool(config.get("RAZORPAY_KEY_ID") and config.get("RAZORPAY_KEY_SECRET"))

    @staticmethod
    def get_currency() -> str:
        return (current_app.config.get("ORG_PRICE_CURRENCY") or "INR").upper()

    @staticmethod
    def get_per_user_price_minor() -> int:
        raw_value = current_app.config.get("ORG_PRICE_PER_ADDITIONAL_USER", "199")
        try:
            amount = Decimal(str(raw_value))
        except (InvalidOperation, TypeError) as exc:  # pragma: no cover - config errors only
            raise BillingConfigurationError("Invalid per-user price configuration") from exc
        cents = (amount * Decimal(100)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        if cents <= 0:
            raise BillingConfigurationError("Per-user price must be greater than zero")
        return int(cents)

    @staticmethod
    def build_pricing_breakdown(organization: Organization, desired_limit: int | None = None) -> PricingBreakdown:
        current_limit = organization.user_limit or OrganizationService.default_user_limit()
        target_limit = desired_limit if desired_limit and desired_limit > 0 else current_limit
        additional = max(target_limit - current_limit, 0)
        per_user_minor = OrganizationBillingService.get_per_user_price_minor()
        total_amount = additional * per_user_minor
        return PricingBreakdown(
            currency=OrganizationBillingService.get_currency(),
            current_limit=current_limit,
            desired_limit=target_limit,
            additional_users=additional,
            per_user_price_minor=per_user_minor,
            total_amount_minor=total_amount,
        )

    @staticmethod
    def _client():
        if not OrganizationBillingService.is_configured():
            raise BillingConfigurationError("Razorpay credentials are not configured")
        import razorpay  # Imported lazily to avoid dependency during tests without billing

        client = razorpay.Client(
            auth=(current_app.config["RAZORPAY_KEY_ID"], current_app.config["RAZORPAY_KEY_SECRET"])
        )
        client.set_app_details({"title": current_app.config.get("APP_NAME", "Finvela"), "version": current_app.config.get("VERSION", "dev")})
        return client

    @staticmethod
    def create_checkout_order(admin: User, organization: Organization, desired_limit: int) -> dict[str, Any]:
        pricing = OrganizationBillingService.build_pricing_breakdown(organization, desired_limit)
        if not pricing.has_change:
            raise BillingError("Select a user limit higher than your current allowance to upgrade.")
        client = OrganizationBillingService._client()
        receipt = f"org-{organization.id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        notes = {
            "organization_id": str(organization.id),
            "current_limit": str(pricing.current_limit),
            "desired_limit": str(pricing.desired_limit),
            "additional_users": str(pricing.additional_users),
            "requested_by": str(admin.id),
        }
        order = client.order.create(
            {
                "amount": pricing.total_amount_minor,
                "currency": pricing.currency,
                "payment_capture": 1,
                "receipt": receipt,
                "notes": notes,
            }
        )
        return {
            "order_id": order.get("id"),
            "amount": int(order.get("amount", pricing.total_amount_minor)),
            "currency": order.get("currency", pricing.currency),
            "notes": order.get("notes", notes),
            "receipt": order.get("receipt", receipt),
            "pricing": pricing.as_dict(),
            "customer": {
                "name": admin.full_name,
                "email": admin.email,
            },
        }

    @staticmethod
    def verify_and_record_payment(
        admin: User,
        organization: Organization,
        *,
        order_id: str,
        payment_id: str,
        signature: str,
        desired_limit: int,
    ) -> OrganizationSubscription:
        pricing = OrganizationBillingService.build_pricing_breakdown(organization, desired_limit)
        if not pricing.has_change:
            raise BillingError("No additional seats selected for upgrade.")
        client = OrganizationBillingService._client()
        try:
            client.utility.verify_payment_signature(
                {
                    "razorpay_order_id": order_id,
                    "razorpay_payment_id": payment_id,
                    "razorpay_signature": signature,
                }
            )
        except Exception as exc:  # pragma: no cover - depends on external SDK
            raise PaymentVerificationError("Payment signature verification failed.") from exc

        payment = client.payment.fetch(payment_id)
        status = (payment or {}).get("status", "")
        if status not in {"captured", "authorized"}:
            raise PaymentVerificationError(f"Unexpected payment status: {status or 'unknown'}")

        paid_amount = int(payment.get("amount", 0))
        if paid_amount < pricing.total_amount_minor:
            raise PaymentVerificationError("Captured amount does not match expected total.")

        previous_limit = organization.user_limit or OrganizationService.default_user_limit()
        organization.user_limit = pricing.desired_limit
        organization.is_premium = True
        organization.last_payment_at = datetime.utcnow()
        if organization.premium_since is None:
            organization.premium_since = organization.last_payment_at

        subscription = OrganizationSubscription(
            organization=organization,
            created_by=admin,
            order_id=order_id,
            payment_id=payment_id,
            signature=signature,
            currency=pricing.currency,
            amount_minor=paid_amount,
            per_user_price_minor=pricing.per_user_price_minor,
            previous_user_limit=previous_limit,
            purchased_user_limit=pricing.desired_limit,
            additional_users=pricing.additional_users,
            notes={
                "email": payment.get("email"),
                "contact": payment.get("contact"),
                "method": payment.get("method"),
                "fee": payment.get("fee"),
                "tax": payment.get("tax"),
            },
        )
        db.session.add(subscription)
        db.session.add(organization)
        db.session.commit()
        return subscription

    @staticmethod
    def format_currency(minor_units: int, currency: str | None = None) -> str:
        currency = (currency or OrganizationBillingService.get_currency()).upper()
        value = Decimal(minor_units) / Decimal(100)
        return f"{currency} {value.quantize(Decimal('0.01'))}"

    @staticmethod
    def serialize_transaction(tx: OrganizationSubscription) -> dict[str, Any]:
        return {
            "order_id": tx.order_id,
            "payment_id": tx.payment_id,
            "amount_minor": tx.amount_minor,
            "currency": tx.currency,
            "per_user_price_minor": tx.per_user_price_minor,
            "additional_users": tx.additional_users,
            "purchased_user_limit": tx.purchased_user_limit,
            "previous_user_limit": tx.previous_user_limit,
            "created_at": tx.created_at,
            "notes": tx.notes or {},
        }
