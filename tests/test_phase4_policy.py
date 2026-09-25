from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from student_agent.contracts import ContractError, Contracts
from student_agent.trace import TraceWriter
from student_agent.workflow import (
    ALL_TOOLS,
    CaseContext,
    PolicyEngine,
    ToolBroker,
    VerifierAgent,
    _build_public_output,
    _financial_resolution,
    _normalize_payment_evidence,
    _normalize_shipment_evidence,
    _verify_output,
    solve_case,
)

ROOT = Path(__file__).resolve().parents[1]


def evidence(ref_number: int, domain: str, data: Any) -> dict[str, Any]:
    tool_name = f"{domain}-{ref_number}"
    return {
        "schema_version": "day09-mcp-evidence-v1",
        "evidence_ref": f"ev_{domain}_{ref_number:020d}",
        "result_hash": "sha256:" + hashlib.sha256(tool_name.encode()).hexdigest(),
        "domain": domain,
        "data": data,
    }


def context_for(
    *,
    shipment: dict[str, Any] | None = None,
    payment: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    claims: list[dict[str, Any]] | None = None,
    conflict: dict[str, Any] | None = None,
    order_status: str = "delivered",
) -> CaseContext:
    order = evidence(
        1,
        "order",
        {"order_id": "order-1", "customer_unique_id": "customer-1", "order_status": order_status},
    )
    item = evidence(
        2,
        "item",
        {
            "order_id": "order-1",
            "items": [
                {"order_item_id": "item-1", "product_id": "product-1", "seller_id": "seller-1"}
            ],
        },
    )
    shipment_evidence = [evidence(3, "shipment", shipment)] if shipment is not None else []
    payment_evidence = [evidence(4, "payment", payment)] if payment is not None else []
    policy_evidence = [evidence(5, "policy", policy or {"policy_version": "POLICY_V1"})]
    if conflict is not None:
        shipment_evidence[0]["data"]["conflicts"] = [conflict]

    all_evidence = [order, item, *shipment_evidence, *payment_evidence, *policy_evidence]
    context = CaseContext(
        case_id="L3B_PHASE4_TEST",
        case={
            "case_id": "L3B_PHASE4_TEST",
            "policy_version": "POLICY_V1",
            "customer_request": {"claims": claims or []},
        },
    )
    context.evidence = {item["evidence_ref"]: item for item in all_evidence}
    context.findings = {
        "entity-order-agent": {
            "entity_resolution": {
                "status": "resolved",
                "resolved_order_ids": ["order-1"],
                "rejected_candidates": [],
                "confidence": 1.0,
            },
            "item_ids": ["item-1"],
            "product_ids": ["product-1"],
            "seller_ids": ["seller-1"],
            "evidence": [order, item],
            "evidence_refs": [order["evidence_ref"], item["evidence_ref"]],
        },
        "customer-agent": {
            "customer_context": {"customer_unique_id": None, "related_order_ids": []}
        },
        "shipment-agent": {
            "evidence": shipment_evidence,
            "evidence_refs": [item["evidence_ref"] for item in shipment_evidence],
        },
        "payment-refund-agent": {
            "evidence": payment_evidence,
            "evidence_refs": [item["evidence_ref"] for item in payment_evidence],
        },
        "policy-agent": {
            "evidence": policy_evidence,
            "evidence_refs": [item["evidence_ref"] for item in policy_evidence],
        },
    }
    return context


@pytest.mark.parametrize(
    ("shipment_verdict", "expected_issue", "expected_party"),
    [
        ("seller_delay", "late_delivery_seller", "seller"),
        ("logistics_delay", "late_delivery_logistics", "logistics_provider"),
    ],
)
def test_policy_distinguishes_shipment_root_causes(
    shipment_verdict: str, expected_issue: str, expected_party: str
) -> None:
    context = context_for(
        shipment={
            "shipment_id": "shipment-1",
            "verdict": shipment_verdict,
            "timeline_complete": True,
            "seller_id": "seller-1",
            "logistics_provider_id": "carrier-1",
        }
    )
    decision = PolicyEngine(context).evaluate()
    assert decision["primary_issue"] == expected_issue
    assert decision["root_cause_analysis"]["responsible_parties"][0]["party_type"] == expected_party
    assert len(decision["resolution_actions"]) == len(set(decision["resolution_actions"]))


@pytest.mark.parametrize(
    ("verdict", "expected_issue"),
    [
        ("capture_mismatch", "payment_mismatch"),
        ("duplicate_capture", "duplicate_charge"),
        ("refund_pending", "refund_pending"),
        ("refund_failed", "refund_failed"),
    ],
)
def test_policy_maps_payment_and_refund_verdicts(verdict: str, expected_issue: str) -> None:
    context = context_for(
        payment={
            "payment_reference": "payment-1",
            "verdict": verdict,
            "payment_provider_id": "provider-1",
            "captured_total_brl": 100.0,
            "refunded_total_brl": 0.0,
            "refundable_total_brl": 100.0,
        }
    )
    decision = PolicyEngine(context).evaluate()
    assert decision["primary_issue"] == expected_issue
    assert (
        decision["root_cause_analysis"]["responsible_parties"][0]["party_type"]
        == "payment_provider"
    )


def test_financial_resolution_is_decimal_based_consistent_and_capped() -> None:
    payment = evidence(
        10,
        "payment",
        {
            "captured_total_brl": "100.00",
            "refunded_total_brl": "20.00",
            "refundable_total_brl": "80.00",
            "refund_lines": [
                {"reason_code": "seller_delay", "amount_brl": "12.34", "entity_id": "order-1"}
            ],
        },
    )
    policy = evidence(
        11,
        "policy",
        {"recommended_refund_brl": "12.34", "refund_eligible": True},
    )
    result = _financial_resolution([payment], [policy])
    assert result["recommended_refund_brl"] == 12.34
    assert sum(line["amount_brl"] for line in result["refund_lines"]) == 12.34

    over_cap = dict(policy)
    over_cap["data"] = {"recommended_refund_brl": "90.00"}
    assert _financial_resolution([payment], [over_cap])["recommended_refund_brl"] == 0.0


def test_missing_evidence_and_conflict_reduce_confidence() -> None:
    missing = context_for(shipment=None, payment=None)
    missing_decision = PolicyEngine(missing).evaluate()
    assert missing_decision["primary_issue"] == "insufficient_evidence"
    assert missing_decision["case_status"] == "needs_investigation"
    assert missing_decision["confidence"] <= 0.25

    clean = context_for(
        shipment={"verdict": "seller_delay", "timeline_complete": True, "seller_id": "seller-1"},
        payment={"verdict": "reconciled", "captured_total_brl": 10.0},
    )
    conflicted = context_for(
        shipment={"verdict": "seller_delay", "timeline_complete": True, "seller_id": "seller-1"},
        payment={"verdict": "reconciled", "captured_total_brl": 10.0},
        conflict={
            "field": "shipment.verdict",
            "sources": ["shipment", "carrier"],
            "selected_source": None,
            "resolution_code": "unresolved",
        },
    )
    assert PolicyEngine(clean).evaluate()["confidence"] > missing_decision["confidence"]
    assert (
        PolicyEngine(conflicted).evaluate()["confidence"]
        < PolicyEngine(clean).evaluate()["confidence"]
    )


def test_incomplete_payment_amounts_reduce_confidence() -> None:
    shipment = _shipment_timeline(seller_late=True)
    payment = _payments()
    complete = context_for(
        shipment=shipment,
        payment={
            **payment,
            "captured_total_brl": "100.00",
            "refunded_total_brl": "0.00",
            "refundable_total_brl": "100.00",
        },
        policy=_rules_for("late_delivery_seller", party="seller"),
    )
    incomplete = context_for(
        shipment=shipment,
        payment=payment,
        policy=_rules_for("late_delivery_seller", party="seller"),
    )

    complete_confidence = PolicyEngine(complete).evaluate()["confidence"]
    incomplete_confidence = PolicyEngine(incomplete).evaluate()["confidence"]

    assert complete_confidence > incomplete_confidence


def test_verifier_degrades_cross_field_inconsistency(tmp_path: Path) -> None:
    context = context_for(
        shipment={"verdict": "seller_delay", "timeline_complete": True, "seller_id": "seller-1"},
        payment={"verdict": "reconciled", "captured_total_brl": 10.0},
    )
    decision = PolicyEngine(context).evaluate()
    decision["root_cause_analysis"]["responsible_parties"] = [
        {"party_type": "logistics_provider", "party_id": "carrier-1"}
    ]
    context.findings["policy-agent"]["decision"] = decision
    trace = TraceWriter(tmp_path / "trace.jsonl", Contracts(ROOT / "contracts" / "schemas"))
    broker = ToolBroker(context, _NoCallGateway(), trace)
    message = __import__("student_agent.workflow", fromlist=["A2AMessage"]).A2AMessage.create(
        case_id=context.case_id,
        sender="conflict-resolver",
        recipient="verifier-agent",
        task="verify_case",
        payload={},
    )
    output = asyncio.run(VerifierAgent(context, broker).run(message))
    assert output["assessment"]["primary_issue"] == "insufficient_evidence"
    assert context.findings["verifier-agent"]["issues"]


def test_public_output_rejects_internal_fields() -> None:
    output = _build_public_output(
        context_for(shipment={"verdict": "on_time", "timeline_complete": True})
    )
    output["internal_state"] = {"agent": "policy-agent"}
    with pytest.raises(ContractError):
        Contracts(ROOT / "contracts" / "schemas").validate_output(output, "test")


def test_lifecycle_policy_and_verifier_events_have_case_local_refs(tmp_path: Path) -> None:
    from test_phase3_workflow import FakeGateway, base_case

    contracts = Contracts(ROOT / "contracts" / "schemas")
    trace_path = tmp_path / "trace.jsonl"
    trace = TraceWriter(trace_path, contracts)
    asyncio.run(solve_case(base_case(), FakeGateway(), trace))
    events = [json.loads(line) for line in trace_path.read_text().splitlines()]
    types = [event["event_type"] for event in events]
    assert (
        types.index("case_received")
        < types.index("policy_decided")
        < types.index("verification_completed")
    )
    known = {
        ref
        for event in events
        if event["event_type"] == "tool_result_consumed"
        for ref in event.get("evidence_refs", [])
    }
    for event_type in ("policy_decided", "verification_completed"):
        event = next(event for event in events if event["event_type"] == event_type)
        assert event["case_id"] == "L3B_CASE_TEST"
        assert set(event.get("evidence_refs", [])) <= known
        assert event["actor"] == (
            "policy-agent" if event_type == "policy_decided" else "verifier-agent"
        )
        assert event.get("decision_code")
        assert event.get("evidence_refs")
    first_task = types.index("task_assigned")
    first_result = types.index("tool_result_consumed")
    first_handoff = types.index("handoff")
    assert first_task < first_result < first_handoff < types.index("policy_decided")


def test_nested_camel_case_payload_and_wrapped_business_policy_are_normalized() -> None:
    context = context_for(
        shipment={
            "result": {
                "order": {
                    "deliveredCarrierAt": "2024-01-05T10:00:00-03:00",
                    "deliveredCustomerAt": "2024-01-08T10:00:00-03:00",
                    "estimatedDeliveryAt": "2024-01-06T10:00:00-03:00",
                    "shippingLimits": [
                        {"sellerId": "seller-1", "shippingLimitAt": "2024-01-04T10:00:00-03:00"}
                    ],
                }
            }
        },
        payment={
            "result": {
                "payments": [
                    {
                        "orderId": "order-1",
                        "paymentSequential": 1,
                        "paymentType": "voucher",
                        "paymentValue": "40.00",
                    },
                    {
                        "orderId": "order-1",
                        "paymentSequential": 2,
                        "paymentType": "credit_card",
                        "paymentValue": "60.00",
                    },
                ]
            }
        },
        policy={
            "result": {
                "businessPolicy": {
                    "rules": {
                        "late_delivery_seller": {
                            "case_status": "action_required",
                            "recommended_action": "contact_seller",
                            "responsible_parties": [{"party_type": "seller", "party_id": None}],
                        }
                    }
                }
            }
        },
    )
    shipment = _normalize_shipment_evidence(context.findings["shipment-agent"]["evidence"])
    payment = _normalize_payment_evidence(context.findings["payment-refund-agent"]["evidence"])
    assert shipment["verdict"] == "seller_delay"
    assert shipment["timeline_complete"] is True
    assert payment["verdict"] == "valid_split_payment"
    assert payment["captured_total_brl"] == 100
    decision = PolicyEngine(context).evaluate()
    assert decision["primary_issue"] == "late_delivery_seller"
    assert decision["resolution_actions"] == ["contact_seller"]


def test_refund_recommendation_without_payment_ref_is_removed_by_financial_policy() -> None:
    policy = evidence(
        30,
        "policy",
        {"rules": {"canceled_order_paid": {"refund_brl": "75.00"}}},
    )
    result = _financial_resolution(
        [],
        [policy],
        primary_issue="canceled_order_paid",
        policy_rule={"refund_brl": "75.00"},
        entity_ids=["order-1"],
    )
    assert result == {"currency": "BRL", "recommended_refund_brl": 0.0, "refund_lines": []}


def test_verifier_checks_complete_contract_and_primary_support() -> None:
    context = context_for(
        shipment=_shipment_timeline(seller_late=True),
        payment=_payments(),
        policy=_rules_for("late_delivery_seller", party="seller"),
    )
    output = _build_public_output(context)
    output["unsupported_private_value"] = True
    contracts = Contracts(ROOT / "contracts" / "schemas")
    assert "schema_invalid" in _verify_output(context, output, contracts=contracts)
    output = _build_public_output(context)
    output["assessment"]["primary_issue"] = "duplicate_charge"
    assert "payment_issue_not_supported" in _verify_output(context, output)


def _rules_for(issue: str, *, refund: str = "0.00", party: str = "unknown") -> dict[str, Any]:
    return {
        "policy_version": "POLICY_V1",
        "rules": {
            issue: {
                "case_status": "action_required",
                "recommended_action": f"handle_{issue}",
                "refund_brl": refund,
                "responsible_parties": [{"party_type": party, "party_id": None}],
            }
        },
    }


def _shipment_timeline(
    *, seller_late: bool = False, logistics_late: bool = False
) -> dict[str, Any]:
    return {
        "order_id": "order-1",
        "order_status": "delivered",
        "delivered_carrier_at": "2024-01-05T10:00:00-03:00"
        if seller_late
        else "2024-01-03T10:00:00-03:00",
        "delivered_customer_at": "2024-01-08T10:00:00-03:00"
        if logistics_late
        else "2024-01-04T10:00:00-03:00",
        "estimated_delivery_at": "2024-01-06T10:00:00-03:00",
        "shipping_limits": [
            {"seller_id": "seller-1", "shipping_limit_at": "2024-01-04T10:00:00-03:00"}
        ],
        "events": [],
    }


def _payments(
    *, event_type: str | None = None, status: str | None = None, amount: str = "100.00"
) -> dict[str, Any]:
    event = (
        {}
        if event_type is None
        else {"event_type": event_type, "status": status or "", "amount_brl": amount}
    )
    return {
        "order_id": "order-1",
        "payments": [
            {
                "order_id": "order-1",
                "payment_sequential": 1,
                "payment_type": "credit_card",
                "payment_value": "100.00",
            }
        ],
        "events": [event] if event else [],
    }


@pytest.mark.parametrize(
    ("order_status", "issue"),
    [("canceled", "canceled_order_paid"), ("unavailable", "unavailable_order_paid")],
)
def test_nested_order_status_with_paid_evidence_is_decisive(order_status: str, issue: str) -> None:
    context = context_for(
        payment=_payments(),
        policy=_rules_for(issue, refund="100.00", party="platform"),
        order_status=order_status,
    )
    decision = PolicyEngine(context).evaluate()
    assert decision["primary_issue"] == issue
    assert decision["financial_resolution"]["recommended_refund_brl"] == 100.0
    assert decision["financial_resolution"]["refund_lines"][0]["entity_id"] == "order-1"


@pytest.mark.parametrize(
    ("shipment", "issue", "party"),
    [
        (_shipment_timeline(seller_late=True), "late_delivery_seller", "seller"),
        (_shipment_timeline(logistics_late=True), "late_delivery_logistics", "logistics_provider"),
    ],
)
def test_nested_shipment_timeline_assigns_correct_party(
    shipment: dict[str, Any], issue: str, party: str
) -> None:
    decision = PolicyEngine(
        context_for(shipment=shipment, payment=_payments(), policy=_rules_for(issue, party=party))
    ).evaluate()
    assert decision["primary_issue"] == issue
    assert decision["shipment_analysis"]["timeline_complete"] is True
    assert decision["responsible_parties"][0]["party_type"] == party


@pytest.mark.parametrize(
    ("payment", "issue"),
    [
        (
            {
                "payments": [
                    {
                        "order_id": "order-1",
                        "payment_sequential": 1,
                        "payment_type": "voucher",
                        "payment_value": "40.00",
                    },
                    {
                        "order_id": "order-1",
                        "payment_sequential": 2,
                        "payment_type": "credit_card",
                        "payment_value": "60.00",
                    },
                ],
                "events": [],
            },
            "valid_split_payment",
        ),
        (_payments(event_type="captured", status="captured", amount="90.00"), "payment_mismatch"),
        (_payments(event_type="duplicate_charge", status="captured"), "duplicate_charge"),
        (_payments(event_type="refund", status="pending"), "refund_pending"),
        (_payments(event_type="refund", status="failed"), "refund_failed"),
    ],
)
def test_nested_payment_timeline_maps_all_payment_scenarios(
    payment: dict[str, Any], issue: str
) -> None:
    decision = PolicyEngine(
        context_for(payment=payment, policy=_rules_for(issue, party="payment_provider"))
    ).evaluate()
    assert decision["primary_issue"] == issue
    assert decision["payment_analysis"]["captured_total_brl"] is not None


def test_unsupported_claim_and_ambiguous_evidence_are_calibrated() -> None:
    unsupported = context_for(
        shipment=_shipment_timeline(),
        payment=_payments(),
        policy=_rules_for("unsupported_claim"),
        claims=[
            {"claim_id": "claim-1", "topic": "late_delivery_seller", "description": "late delivery"}
        ],
    )
    decision = PolicyEngine(unsupported).evaluate()
    assert decision["primary_issue"] == "unsupported_claim"
    assert decision["claim_assessments"][0]["verdict"] == "unsupported"
    ambiguous = context_for(payment=_payments())
    ambiguous.findings["entity-order-agent"]["entity_resolution"]["status"] = "ambiguous"
    ambiguous_decision = PolicyEngine(ambiguous).evaluate()
    assert ambiguous_decision["case_status"] == "needs_investigation"
    assert ambiguous_decision["confidence"] <= 0.45


def test_verifier_detects_refund_and_responsibility_inconsistencies() -> None:
    context = context_for(
        shipment=_shipment_timeline(seller_late=True),
        payment=_payments(),
        policy=_rules_for("late_delivery_seller", party="seller"),
    )
    output = _build_public_output(context)
    output["financial_resolution"] = {
        "currency": "BRL",
        "recommended_refund_brl": 150.0,
        "refund_lines": [{"reason_code": "test", "amount_brl": 100.0, "entity_id": "order-1"}],
    }
    output["root_cause_analysis"]["responsible_parties"] = [
        {"party_type": "logistics_provider", "party_id": "carrier-1"}
    ]
    from student_agent.workflow import _verify_output

    issues = _verify_output(context, output)
    assert "refund_lines_total_mismatch" in issues
    assert "seller_logistics_responsibility_conflict" in issues
    output["root_cause_analysis"]["responsible_parties"] = [
        {"party_type": "invalid", "party_id": None}
    ]
    assert "invalid_responsible_party" in _verify_output(context, output)


def test_valid_split_payment_keeps_public_payment_enum_valid() -> None:
    payment = {
        "payments": [
            {
                "order_id": "order-1",
                "payment_sequential": 1,
                "payment_type": "voucher",
                "payment_value": "40.00",
            },
            {
                "order_id": "order-1",
                "payment_sequential": 2,
                "payment_type": "credit_card",
                "payment_value": "60.00",
            },
        ],
        "events": [],
    }
    output = _build_public_output(
        context_for(payment=payment, policy=_rules_for("valid_split_payment"))
    )
    assert output["assessment"]["primary_issue"] == "valid_split_payment"
    assert output["payment_analysis"]["verdict"] == "reconciled"
    Contracts(ROOT / "contracts" / "schemas").validate_output(output, "split-payment")


class _NoCallGateway:
    async def list_tools(self) -> list[str]:
        return list(ALL_TOOLS)

    async def call(self, tool_name: str, *, case_id: str, **arguments: str) -> dict[str, Any]:
        raise AssertionError(f"unexpected MCP call: {tool_name} {case_id} {arguments}")
