from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from student_agent.contracts import Contracts
from student_agent.trace import TraceWriter
from student_agent.workflow import (
    ALL_TOOLS,
    TOOL_PERMISSIONS,
    CaseContext,
    ToolBroker,
    WorkflowError,
    solve_case,
)

ROOT = Path(__file__).resolve().parents[1]
ORDER_ID = "order-1"


TOOL_DOMAINS = {
    "get_customer_history": "customer",
    "get_order": "order",
    "get_order_items": "item",
    "get_order_payments": "payment",
    "get_payment_timeline": "payment",
    "get_policy": "policy",
    "get_product_context": "product",
    "get_refund_timeline": "refund",
    "get_sellers": "seller",
    "get_shipment_summary": "shipment",
}


class FakeGateway:
    def __init__(
        self,
        *,
        tools: list[str] | None = None,
        failures: dict[str, str] | None = None,
        malformed: set[str] | None = None,
    ) -> None:
        self.tools = tools if tools is not None else sorted(ALL_TOOLS)
        self.failures = failures or {}
        self.malformed = malformed or set()
        self.calls: list[tuple[str, str, dict[str, str]]] = []
        self._sequence = 0
        self.evidence_refs: set[str] = set()

    async def list_tools(self) -> list[str]:
        return list(self.tools)

    async def call(self, tool_name: str, *, case_id: str, **arguments: str) -> dict[str, Any]:
        self.calls.append((tool_name, case_id, arguments))
        failure = self.failures.get(tool_name)
        if failure is not None:
            raise RuntimeError(failure)
        if tool_name in self.malformed:
            return {"not": "an evidence envelope"}

        self._sequence += 1
        evidence_ref = f"ev_{tool_name}_{self._sequence:020d}"
        self.evidence_refs.add(evidence_ref)
        return {
            "schema_version": "day09-mcp-evidence-v1",
            "evidence_ref": evidence_ref,
            "result_hash": "sha256:" + hashlib.sha256(tool_name.encode()).hexdigest(),
            "domain": TOOL_DOMAINS[tool_name],
            "data": self._data(tool_name, arguments),
        }

    @staticmethod
    def _data(tool_name: str, arguments: dict[str, str]) -> dict[str, Any]:
        if tool_name == "get_order":
            return {"order_id": arguments["order_id"], "customer_unique_id": "customer-1"}
        if tool_name == "get_order_items":
            return {
                "order_id": arguments["order_id"],
                "items": [
                    {"order_item_id": "item-1", "product_id": "product-1", "seller_id": "seller-1"}
                ],
            }
        if tool_name == "get_customer_history":
            return {"customer_unique_id": arguments["customer_unique_id"], "order_id": ORDER_ID}
        if tool_name == "get_product_context":
            return {"order_id": arguments["order_id"], "product_id": "product-1"}
        if tool_name == "get_sellers":
            return {"order_id": arguments["order_id"], "seller_id": "seller-1"}
        if tool_name == "get_shipment_summary":
            return {"shipment_id": "shipment-1", "verdict": "on_time", "timeline_complete": True}
        if tool_name == "get_order_payments":
            return {"payment_reference": "payment-1", "captured_total_brl": 10.0}
        if tool_name == "get_payment_timeline":
            return {"payment_reference": "payment-1", "verdict": "reconciled"}
        if tool_name == "get_refund_timeline":
            return {"payment_reference": "payment-1", "refunded_total_brl": 0.0}
        return {"policy_version": arguments.get("policy_version")}


def base_case(**overrides: Any) -> dict[str, Any]:
    case = {
        "case_id": "L3B_CASE_TEST",
        "customer_request": {"claims": []},
        "candidate_order_ids": [ORDER_ID],
        "customer_unique_id_hint": "customer-1",
        "policy_version": "EC_POLICY_V2",
        "investigation_scope": {
            "include_customer_history": True,
            "include_product_context": True,
        },
    }
    case.update(overrides)
    return case


def run_case(
    case: dict[str, Any], gateway: FakeGateway, tmp_path: Path
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    contracts = Contracts(ROOT / "contracts" / "schemas")
    trace = TraceWriter(tmp_path / "trace.jsonl", contracts)
    output = asyncio.run(solve_case(case, gateway, trace))
    events = [json.loads(line) for line in (tmp_path / "trace.jsonl").read_text().splitlines()]
    return output, events


def test_specialists_preserve_case_id_refs_and_trace_permissions(tmp_path: Path) -> None:
    gateway = FakeGateway()
    output, events = run_case(base_case(), gateway, tmp_path)
    contracts = Contracts(ROOT / "contracts" / "schemas")
    contracts.validate_output(output, "output")
    for event in events:
        contracts.validate_trace(event, "trace")
        assert event["case_id"] == "L3B_CASE_TEST"
    assert all(case_id == "L3B_CASE_TEST" for _, case_id, _ in gateway.calls)
    assert all(
        arguments == {"order_id": ORDER_ID}
        for tool, _, arguments in gateway.calls
        if tool in {"get_product_context", "get_sellers"}
    )
    consumed = [event for event in events if event["event_type"] == "tool_result_consumed"]
    assert {ref for event in consumed for ref in event["evidence_refs"]} <= gateway.evidence_refs
    assert set(output["evidence_refs"]) <= gateway.evidence_refs
    for event in consumed:
        assert event["tool_name"] in TOOL_PERMISSIONS[event["actor"]]
    assert all(event["target"] for event in events if event["event_type"] == "handoff")


def test_customer_scope_and_policy_version_are_enforced(tmp_path: Path) -> None:
    gateway = FakeGateway()
    case = base_case(
        policy_version="POLICY_TEST_V7",
        investigation_scope={"include_customer_history": False, "include_product_context": False},
    )
    output, _ = run_case(case, gateway, tmp_path)
    assert "get_customer_history" not in {tool for tool, _, _ in gateway.calls}
    policy_calls = [arguments for tool, _, arguments in gateway.calls if tool == "get_policy"]
    assert policy_calls == [{"policy_version": "POLICY_TEST_V7"}]
    assert output["customer_context"] == {"customer_unique_id": None, "related_order_ids": []}


def test_failure_is_bounded_and_does_not_emit_consumption(tmp_path: Path) -> None:
    gateway = FakeGateway(
        tools=["get_order"], failures={"get_order": "temporary transport failure"}
    )
    output, events = run_case(base_case(policy_version=None), gateway, tmp_path)
    assert len(gateway.calls) == 2
    assert output["evidence_refs"] == []
    assert not any(event["event_type"] == "tool_result_consumed" for event in events)


def test_non_retryable_authorization_failure_is_not_retried(tmp_path: Path) -> None:
    gateway = FakeGateway(tools=["get_order"], failures={"get_order": "authorization failed"})
    run_case(base_case(policy_version=None), gateway, tmp_path)
    assert len(gateway.calls) == 1


def test_cache_is_case_local_and_consumption_is_deduplicated(tmp_path: Path) -> None:
    async def exercise() -> tuple[int, int, int]:
        gateway = FakeGateway(tools=["get_order"])
        contracts = Contracts(ROOT / "contracts" / "schemas")
        trace = TraceWriter(tmp_path / "cache-trace.jsonl", contracts)
        first = CaseContext("L3B_CASE_A", base_case(case_id="L3B_CASE_A"))
        first_broker = ToolBroker(first, gateway, trace)
        await first_broker.discover()
        await first_broker.call("entity-order-agent", "get_order", arguments={"order_id": ORDER_ID})
        await first_broker.call("entity-order-agent", "get_order", arguments={"order_id": ORDER_ID})
        second = CaseContext("L3B_CASE_B", base_case(case_id="L3B_CASE_B"))
        second_broker = ToolBroker(second, gateway, trace)
        await second_broker.discover()
        await second_broker.call(
            "entity-order-agent", "get_order", arguments={"order_id": ORDER_ID}
        )
        events = (tmp_path / "cache-trace.jsonl").read_text().splitlines()
        return len(gateway.calls), len(events), len(first.consumed_evidence_refs)

    calls, events, first_consumptions = asyncio.run(exercise())
    assert calls == 2
    assert events == 2
    assert first_consumptions == 1


def test_permission_denial_and_invalid_envelope_do_not_call_or_trace(tmp_path: Path) -> None:
    async def exercise() -> tuple[int, int]:
        gateway = FakeGateway(tools=sorted(ALL_TOOLS), malformed={"get_order"})
        contracts = Contracts(ROOT / "contracts" / "schemas")
        trace = TraceWriter(tmp_path / "permission-trace.jsonl", contracts)
        context = CaseContext("L3B_CASE_TEST", base_case())
        broker = ToolBroker(context, gateway, trace)
        await broker.discover()
        with pytest.raises(WorkflowError):
            await broker.call("customer-agent", "get_order", arguments={"order_id": ORDER_ID})
        assert (
            await broker.call("entity-order-agent", "get_order", arguments={"order_id": ORDER_ID})
            is None
        )
        trace_path = tmp_path / "permission-trace.jsonl"
        event_count = len(trace_path.read_text().splitlines()) if trace_path.exists() else 0
        return len(gateway.calls), event_count

    calls, events = asyncio.run(exercise())
    assert calls == 1
    assert events == 0
