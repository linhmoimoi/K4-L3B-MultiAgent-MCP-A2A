from __future__ import annotations

import asyncio
import json
import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, ClassVar

from .cases import CASE_ID_PATTERN
from .mcp_gateway import EvidenceGateway, EvidenceGatewayError
from .trace import TraceWriter

TOOL_TIMEOUT_SECONDS = 15.0
MAX_TOOL_RETRIES = 1
MAX_TOOL_CALLS_PER_CASE = 32

ALL_TOOLS = frozenset(
    {
        "get_customer_history",
        "get_order",
        "get_order_items",
        "get_order_payments",
        "get_payment_timeline",
        "get_policy",
        "get_product_context",
        "get_refund_timeline",
        "get_sellers",
        "get_shipment_summary",
    }
)

TOOL_PERMISSIONS: dict[str, frozenset[str]] = {
    "entity-order-agent": frozenset(
        {"get_order", "get_order_items", "get_sellers", "get_product_context"}
    ),
    "customer-agent": frozenset({"get_customer_history"}),
    "coordinator": frozenset(),
    "shipment-agent": frozenset({"get_shipment_summary"}),
    "payment-refund-agent": frozenset(
        {"get_order_payments", "get_payment_timeline", "get_refund_timeline"}
    ),
    "policy-agent": frozenset({"get_policy"}),
    "conflict-resolver": frozenset(),
    "verifier-agent": frozenset(),
}

ORDER_ID_KEYS = ("order_id", "orderId")
CUSTOMER_ID_KEYS = ("customer_unique_id", "customer_id", "customerId")
ITEM_ID_KEYS = ("order_item_id", "item_id", "itemId")
SELLER_ID_KEYS = ("seller_id", "sellerId")
PAYMENT_ID_KEYS = ("payment_id", "payment_reference", "payment_ref", "payment_sequential")
SHIPMENT_ID_KEYS = ("shipment_id", "shipmentId", "tracking_code", "trackingCode")
PRODUCT_ID_KEYS = ("product_id", "productId")

PRIMARY_ISSUES = frozenset(
    {
        "canceled_order_paid",
        "unavailable_order_paid",
        "late_delivery_seller",
        "late_delivery_logistics",
        "valid_split_payment",
        "payment_mismatch",
        "duplicate_charge",
        "refund_pending",
        "refund_failed",
        "unsupported_claim",
        "insufficient_evidence",
    }
)
RESPONSIBLE_PARTY_TYPES = frozenset(
    {"seller", "platform", "logistics_provider", "payment_provider", "customer", "unknown"}
)
CASE_STATUSES = frozenset({"action_required", "no_action", "needs_investigation"})
CLAIM_VERDICTS = frozenset(
    {"supported", "unsupported", "partially_supported", "insufficient_evidence"}
)
MONEY_QUANTUM = Decimal("0.01")


class WorkflowError(RuntimeError):
    """An internal orchestration error that must not leak into public output."""


@dataclass(frozen=True)
class A2AMessage:
    """Internal handoff envelope; it is never serialized to a public artifact."""

    case_id: str
    message_id: str
    sender: str
    recipient: str
    task: str
    payload: dict[str, Any]
    evidence_refs: tuple[str, ...] = ()
    normalized_data: dict[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        case_id: str,
        sender: str,
        recipient: str,
        task: str,
        payload: dict[str, Any],
        evidence_refs: list[str] | tuple[str, ...] = (),
        normalized_data: dict[str, Any] | None = None,
        warnings: list[str] | tuple[str, ...] = (),
        errors: list[str] | tuple[str, ...] = (),
    ) -> A2AMessage:
        return cls(
            case_id=case_id,
            message_id=f"msg_{secrets.token_urlsafe(12)}",
            sender=sender,
            recipient=recipient,
            task=task,
            payload=payload,
            evidence_refs=tuple(dict.fromkeys(evidence_refs)),
            normalized_data=normalized_data or {},
            warnings=tuple(dict.fromkeys(warnings)),
            errors=tuple(dict.fromkeys(errors)),
        )


@dataclass
class CaseContext:
    case_id: str
    case: dict[str, Any]
    discovered_tools: frozenset[str] = frozenset()
    cache: dict[str, dict[str, Any] | None] = field(default_factory=dict)
    evidence: dict[str, dict[str, Any]] = field(default_factory=dict)
    consumed_evidence_refs: set[str] = field(default_factory=set)
    tool_calls: int = 0
    findings: dict[str, dict[str, Any]] = field(default_factory=dict)

    def cache_key(self, tool_name: str, arguments: dict[str, str]) -> str:
        encoded = json.dumps(arguments, sort_keys=True, separators=(",", ":"))
        return f"{tool_name}:{encoded}"


class ToolBroker:
    """The only path by which an agent may consume MCP evidence."""

    def __init__(self, context: CaseContext, gateway: EvidenceGateway, trace: TraceWriter) -> None:
        self.context = context
        self.gateway = gateway
        self.trace = trace

    async def discover(self) -> frozenset[str]:
        discovered = await self.gateway.list_tools()
        self.context.discovered_tools = frozenset(discovered)
        return self.context.discovered_tools

    async def call(
        self, agent: str, tool_name: str, *, arguments: dict[str, str]
    ) -> dict[str, Any] | None:
        allowed = TOOL_PERMISSIONS.get(agent, frozenset())
        if tool_name not in allowed:
            raise WorkflowError(f"{agent} is not permitted to call {tool_name}")
        if tool_name not in self.context.discovered_tools or tool_name not in ALL_TOOLS:
            return None

        normalized = {key: str(value) for key, value in arguments.items() if value is not None}
        cache_key = self.context.cache_key(tool_name, normalized)
        if cache_key in self.context.cache:
            evidence = self.context.cache[cache_key]
            if evidence is not None:
                self._emit_consumed(agent, tool_name, evidence)
            return evidence
        if self.context.tool_calls >= MAX_TOOL_CALLS_PER_CASE:
            self.context.cache[cache_key] = None
            return None

        evidence: dict[str, Any] | None = None
        for attempt in range(MAX_TOOL_RETRIES + 1):
            self.context.tool_calls += 1
            try:
                result = await asyncio.wait_for(
                    self.gateway.call(tool_name, case_id=self.context.case_id, **normalized),
                    timeout=TOOL_TIMEOUT_SECONDS,
                )
                if not isinstance(result, dict):
                    raise ValueError("MCP response must be an object")
                validator = getattr(
                    getattr(self.trace, "contracts", None), "validate_evidence", None
                )
                if validator is not None:
                    validator(result, f"MCP tool {tool_name}")
                evidence = result
                break
            except asyncio.CancelledError:
                raise
            except EvidenceGatewayError as exc:
                if not exc.retryable or attempt + 1 == MAX_TOOL_RETRIES + 1:
                    break
            except (TimeoutError, OSError, RuntimeError, ConnectionError) as exc:
                if not _is_retryable_exception(exc) or attempt + 1 == MAX_TOOL_RETRIES + 1:
                    break
            except (TypeError, ValueError):
                break
            except Exception as exc:
                if not _is_retryable_exception(exc) or attempt + 1 == MAX_TOOL_RETRIES + 1:
                    break

        self.context.cache[cache_key] = evidence
        if evidence is not None:
            evidence_ref = evidence.get("evidence_ref")
            if not isinstance(evidence_ref, str):
                self.context.cache[cache_key] = None
                return None
            self.context.evidence[evidence_ref] = evidence
            self._emit_consumed(agent, tool_name, evidence)
        return evidence

    def _emit_consumed(self, agent: str, tool_name: str, evidence: dict[str, Any]) -> None:
        evidence_ref = evidence.get("evidence_ref")
        if (
            isinstance(evidence_ref, str)
            and evidence_ref not in self.context.consumed_evidence_refs
        ):
            self.context.consumed_evidence_refs.add(evidence_ref)
            self.trace.emit(
                case_id=self.context.case_id,
                event_type="tool_result_consumed",
                actor=agent,
                tool_name=tool_name,
                evidence_refs=[evidence_ref],
            )


class Agent:
    name: ClassVar[str]

    def __init__(self, context: CaseContext, broker: ToolBroker) -> None:
        self.context = context
        self.broker = broker

    async def tool(self, name: str, **arguments: str) -> dict[str, Any] | None:
        return await self.broker.call(self.name, name, arguments=arguments)

    def validate_message(self, message: A2AMessage) -> None:
        if message.case_id != self.context.case_id:
            raise WorkflowError(f"handoff case mismatch for {self.name}")
        if message.recipient != self.name:
            raise WorkflowError(f"handoff recipient mismatch for {self.name}")


class EntityOrderAgent(Agent):
    name = "entity-order-agent"

    async def run(self, message: A2AMessage) -> dict[str, Any]:
        self.validate_message(message)
        candidate_ids = _candidate_order_ids_from_message(message)
        resolved: list[str] = []
        rejected: list[str] = []
        evidence_items: list[dict[str, Any]] = []
        item_ids: list[str] = []
        product_ids: list[str] = []
        seller_ids: list[str] = []
        customer_ids: list[str] = []

        for order_id in candidate_ids:
            evidence = await self.tool("get_order", order_id=order_id)
            if evidence is None:
                continue
            returned_order_id = _first_value(evidence.get("data"), ORDER_ID_KEYS)
            if returned_order_id != order_id:
                rejected.append(order_id)
                continue
            resolved.append(order_id)
            evidence_items.append(evidence)
            customer_id = _first_value(evidence.get("data"), CUSTOMER_ID_KEYS)
            if customer_id is not None:
                customer_ids.append(customer_id)

        for order_id in _bounded_unique(resolved):
            items = await self.tool("get_order_items", order_id=order_id)
            if items is None:
                continue
            evidence_items.append(items)
            item_ids.extend(_ids_from_evidence(items, ITEM_ID_KEYS))
            product_ids.extend(_ids_from_evidence(items, PRODUCT_ID_KEYS))
            seller_ids.extend(_ids_from_evidence(items, SELLER_ID_KEYS))

        scope = self.context.case.get("investigation_scope", {})
        # The discovered MCP contracts for these tools are order-scoped.  Keep
        # product/seller ids from item evidence as entities, but do not invent
        # undocumented product_id/seller_id tool arguments.
        if scope.get("include_product_context", False):
            for order_id in _bounded_unique(resolved):
                evidence = await self.tool("get_product_context", order_id=order_id)
                if evidence is not None:
                    evidence_items.append(evidence)
        for order_id in _bounded_unique(resolved):
            evidence = await self.tool("get_sellers", order_id=order_id)
            if evidence is not None:
                evidence_items.append(evidence)

        if len(resolved) == 1:
            status, confidence = "resolved", 1.0
        elif len(resolved) > 1:
            status, confidence = "ambiguous", 0.5
        else:
            status, confidence = "not_found", 0.0
        result = {
            "entity_resolution": {
                "status": status,
                "resolved_order_ids": _bounded_unique(resolved),
                "rejected_candidates": _bounded_unique(rejected),
                "confidence": confidence,
            },
            "orders": [item for item in evidence_items if item.get("domain") == "order"],
            "item_ids": _bounded_unique(item_ids),
            "product_ids": _bounded_unique(product_ids),
            "seller_ids": _bounded_unique(seller_ids),
            "customer_id_candidates": _bounded_unique(customer_ids),
            "evidence": evidence_items,
            "evidence_refs": _bounded_unique(_refs(*evidence_items)),
        }
        self.context.findings[self.name] = result
        return result


class CustomerAgent(Agent):
    name = "customer-agent"

    async def run(self, message: A2AMessage) -> dict[str, Any]:
        self.validate_message(message)
        scope = self.context.case.get("investigation_scope", {})
        if scope.get("include_customer_history") is not True:
            result = {
                "customer_context": {"customer_unique_id": None, "related_order_ids": []},
                "evidence": [],
                "evidence_refs": [],
            }
            self.context.findings[self.name] = result
            return result

        customer_ids = _bounded_unique(message.payload.get("customer_id_candidates", []))
        hint = message.payload.get("customer_unique_id_hint")
        if not customer_ids and isinstance(hint, str) and hint:
            customer_ids = [hint]
        evidence_items: list[dict[str, Any]] = []
        for customer_id in customer_ids:
            evidence = await self.tool("get_customer_history", customer_unique_id=customer_id)
            if evidence is not None:
                evidence_items.append(evidence)

        confirmed_customer_id = _first_value_from_evidence(evidence_items, CUSTOMER_ID_KEYS)
        related_order_ids = _ids_from_evidence_list(evidence_items, ORDER_ID_KEYS)
        if confirmed_customer_id:
            related_order_ids = _bounded_unique(
                [*related_order_ids, *_order_ids_from_message(message)]
            )
        result = {
            "customer_context": {
                "customer_unique_id": confirmed_customer_id,
                "related_order_ids": related_order_ids,
            },
            "evidence": evidence_items,
            "evidence_refs": _bounded_unique(_refs(*evidence_items)),
        }
        self.context.findings[self.name] = result
        return result


# Compatibility name for callers that imported the old Phase 2 class.  The
# implementation and permission scope are now the explicit Entity/Order Agent.
OrderItemAgent = EntityOrderAgent


class ShipmentAgent(Agent):
    name = "shipment-agent"

    async def run(self, message: A2AMessage) -> dict[str, Any]:
        self.validate_message(message)
        evidence_items: list[dict[str, Any]] = []
        for order_id in _order_ids_from_message(message):
            evidence = await self.tool("get_shipment_summary", order_id=order_id)
            if evidence is not None:
                evidence_items.append(evidence)
        analysis = _normalize_shipment_evidence(evidence_items)
        result = {
            "evidence": evidence_items,
            "evidence_refs": _bounded_unique(_refs(*evidence_items)),
            "verdict": analysis["verdict"],
            "analysis": analysis,
        }
        self.context.findings[self.name] = result
        return result


class PaymentRefundAgent(Agent):
    name = "payment-refund-agent"

    async def run(self, message: A2AMessage) -> dict[str, Any]:
        self.validate_message(message)
        evidence_items: list[dict[str, Any]] = []
        for order_id in _order_ids_from_message(message):
            for tool_name in (
                "get_order_payments",
                "get_payment_timeline",
                "get_refund_timeline",
            ):
                evidence = await self.tool(tool_name, order_id=order_id)
                if evidence is not None:
                    evidence_items.append(evidence)
        analysis = _normalize_payment_evidence(evidence_items)
        result = {
            "evidence": evidence_items,
            "evidence_refs": _bounded_unique(_refs(*evidence_items)),
            "verdict": analysis["verdict"],
            "analysis": analysis,
        }
        self.context.findings[self.name] = result
        return result


class PolicyAgent(Agent):
    name = "policy-agent"

    async def run(self, message: A2AMessage) -> dict[str, Any]:
        self.validate_message(message)
        policy_version = self.context.case.get("policy_version")
        evidence = None
        if isinstance(policy_version, str) and policy_version:
            evidence = await self.tool("get_policy", policy_version=policy_version)
        result = {
            "evidence": [evidence] if evidence is not None else [],
            "evidence_refs": _refs(evidence),
        }
        self.context.findings[self.name] = result
        return result

    def decide(self, message: A2AMessage) -> dict[str, Any]:
        """Apply business policy after all independent specialist findings exist."""
        self.validate_message(message)
        decision = PolicyEngine(self.context).evaluate()
        finding = self.context.findings.setdefault(self.name, {"evidence": [], "evidence_refs": []})
        finding["decision"] = decision
        return decision


class ConfidenceCalibrator:
    """Deterministic confidence model based only on case-local findings."""

    _weights = {
        "entity": 0.18,
        "required_evidence": 0.18,
        "direct_support": 0.22,
        "conflict_free": 0.14,
        "timeline": 0.10,
        "policy": 0.10,
        "specialist_consistency": 0.08,
    }

    def overall(
        self,
        context: CaseContext,
        *,
        primary_issue: str,
        supporting_refs: list[str],
        conflicts: list[dict[str, Any]],
    ) -> float:
        resolution = context.findings.get("entity-order-agent", {}).get("entity_resolution", {})
        entity_status = resolution.get("status")
        reported_entity_confidence = (
            _clamp_confidence(resolution.get("confidence"))
            if "confidence" in resolution
            else {"resolved": 1.0, "ambiguous": 0.35, "not_found": 0.0}.get(entity_status, 0.0)
        )
        entity_score = {
            "resolved": reported_entity_confidence,
            "ambiguous": min(0.35, reported_entity_confidence),
            "not_found": 0.0,
        }.get(entity_status, 0.0)
        required_names = (
            "entity-order-agent",
            "shipment-agent",
            "payment-refund-agent",
            "policy-agent",
        )
        required_score = sum(
            bool(context.findings.get(name, {}).get("evidence_refs")) for name in required_names
        ) / len(required_names)
        direct_score = 1.0 if primary_issue != "insufficient_evidence" and supporting_refs else 0.0
        conflict_score = 0.0 if conflicts else 1.0
        shipment_analysis = _finding_analysis(
            context,
            "shipment-agent",
            _normalize_shipment_evidence(_finding_evidence(context, "shipment-agent")),
        )
        payment_analysis = _finding_analysis(
            context,
            "payment-refund-agent",
            _normalize_payment_evidence(_finding_evidence(context, "payment-refund-agent")),
        )
        timeline_score = (
            float(shipment_analysis.get("timeline_complete") is True)
            + float(payment_analysis.get("verdict") != "insufficient_evidence")
        ) / 2.0
        payment_amount_fields = (
            "captured_total_brl",
            "refunded_total_brl",
            "refundable_total_brl",
        )
        payment_amount_completeness = sum(
            payment_analysis.get(field) is not None for field in payment_amount_fields
        ) / len(payment_amount_fields)
        evidence_completeness = (timeline_score + payment_amount_completeness) / 2.0
        policy_score = float(bool(context.findings.get("policy-agent", {}).get("evidence_refs")))
        consistency_score = 0.0 if _specialist_errors(context) else 1.0
        values = {
            "entity": entity_score,
            "required_evidence": required_score,
            "direct_support": direct_score,
            "conflict_free": conflict_score,
            "timeline": timeline_score,
            "policy": policy_score,
            "specialist_consistency": consistency_score,
        }
        score = sum(self._weights[name] * values[name] for name in self._weights)
        if primary_issue == "insufficient_evidence":
            score = min(score, 0.25)
        if entity_status != "resolved":
            score = min(score, 0.45)
        if conflicts:
            score = min(score, 0.55)
        # The base signal weights describe whether evidence supports a decision.
        # Discount it when the shipment timeline or payment totals are incomplete;
        # otherwise complete records all saturated the old 0.95 ceiling.
        score = min(score, 0.95) * (0.80 + 0.20 * evidence_completeness)
        # MCP evidence can be complete for the observed fields but does not
        # establish omniscience; reserve 1.0 for no production decision.
        return _clamp_confidence(score)

    def claim(self, verdict: str, evidence_refs: list[str], conflicts: bool) -> float:
        base = {
            "supported": 0.86,
            "unsupported": 0.76,
            "partially_supported": 0.62,
            "insufficient_evidence": 0.12,
        }.get(verdict, 0.0)
        if not evidence_refs:
            return 0.0
        if conflicts:
            base *= 0.55
        return _clamp_confidence(base)


class PolicyEngine:
    """Maps case-local evidence and MCP policy evidence to public decisions."""

    def __init__(self, context: CaseContext) -> None:
        self.context = context
        self.calibrator = ConfidenceCalibrator()

    def evaluate(self) -> dict[str, Any]:
        shipment_evidence = _finding_evidence(self.context, "shipment-agent")
        payment_evidence = _finding_evidence(self.context, "payment-refund-agent")
        policy_evidence = _finding_evidence(self.context, "policy-agent")
        entity = self.context.findings.get("entity-order-agent", {})
        conflicts = _collect_conflicts(self.context)
        shipment_analysis = _finding_analysis(
            self.context, "shipment-agent", _normalize_shipment_evidence(shipment_evidence)
        )
        payment_analysis = _finding_analysis(
            self.context, "payment-refund-agent", _normalize_payment_evidence(payment_evidence)
        )
        shipment_verdict = shipment_analysis["verdict"]
        payment_verdict = payment_analysis["verdict"]
        primary_issue = _choose_primary_issue(
            policy_evidence,
            shipment_evidence,
            payment_evidence,
            context=self.context,
            shipment_analysis=shipment_analysis,
            payment_analysis=payment_analysis,
        )
        policy_rule = _policy_rule(policy_evidence, primary_issue)
        financial = _financial_resolution(
            payment_evidence,
            policy_evidence,
            primary_issue=primary_issue,
            payment_analysis=payment_analysis,
            policy_rule=policy_rule,
            entity_ids=entity.get("entity_resolution", {}).get("resolved_order_ids", []),
        )
        supporting_refs = _supporting_refs(
            primary_issue,
            self.context,
            shipment_evidence,
            payment_evidence,
            policy_evidence,
        )
        data_conflicts = conflicts[:5]
        status = _choose_case_status(
            primary_issue,
            entity,
            policy_evidence,
            data_conflicts,
            policy_rule=policy_rule,
        )
        actions = _policy_actions(
            primary_issue,
            status,
            policy_evidence,
            financial,
            policy_rule=policy_rule,
        )
        claims = _claim_assessments(
            self.context,
            _all_evidence_refs(self.context),
            shipment_verdict=shipment_verdict,
            payment_verdict=payment_verdict,
            financial=financial,
            conflicts=bool(data_conflicts),
            primary_issue=primary_issue,
        )
        responsible = _responsible_parties(
            primary_issue,
            self.context,
            policy_evidence,
            shipment_evidence,
            payment_evidence,
            policy_rule=policy_rule,
        )
        root_cause = _root_cause_for(primary_issue, responsible)
        confidence = self.calibrator.overall(
            self.context,
            primary_issue=primary_issue,
            supporting_refs=supporting_refs,
            conflicts=data_conflicts,
        )
        return {
            "decision_code": (
                "insufficient_evidence"
                if primary_issue == "insufficient_evidence"
                else "conflict_detected"
                if data_conflicts
                else "policy_decision"
            ),
            "primary_issue": primary_issue,
            "case_status": status,
            "secondary_issues": _secondary_issues_from_verdicts(
                shipment_verdict, payment_verdict, primary_issue
            ),
            "confidence": confidence,
            "responsible_parties": responsible,
            "root_cause_analysis": root_cause,
            "financial_resolution": financial,
            "resolution_actions": actions,
            "claim_assessments": claims,
            "shipment_verdict": shipment_verdict,
            "payment_verdict": payment_verdict,
            "shipment_analysis": shipment_analysis,
            "payment_analysis": payment_analysis,
            "evidence_refs": _bounded_unique(supporting_refs),
            "data_conflicts": data_conflicts,
            "warnings": _policy_warnings(financial, data_conflicts),
        }


class ConflictResolver(Agent):
    name = "conflict-resolver"

    async def run(self, message: A2AMessage) -> dict[str, Any]:
        self.validate_message(message)
        conflicts = _collect_conflicts(self.context)
        result = {"data_conflicts": conflicts[:5]}
        self.context.findings[self.name] = result
        return result


class VerifierAgent(Agent):
    name = "verifier-agent"

    async def run(self, message: A2AMessage) -> dict[str, Any]:
        self.validate_message(message)
        output = _build_public_output(self.context)
        contracts = getattr(self.broker.trace, "contracts", None)
        issues = _verify_output(self.context, output, contracts=contracts)
        if issues:
            output = _degrade_output(output, self.context, issues)
        validator = getattr(getattr(self.broker.trace, "contracts", None), "validate_output", None)
        if validator is not None:
            validator(output, f"workflow/{self.context.case_id}")
        self.context.findings[self.name] = {"output": output, "issues": issues}
        return output


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
    """Run the coordinator and MCP-backed specialist handoff graph for one case."""
    if not isinstance(case, dict):
        raise ValueError("case must be an object")
    case_id = case.get("case_id")
    if not isinstance(case_id, str) or not CASE_ID_PATTERN.fullmatch(case_id):
        raise ValueError("case.case_id is missing or invalid")

    context = CaseContext(case_id=case_id, case=case)
    trace.emit(case_id=case_id, event_type="case_received", actor="coordinator")
    broker = ToolBroker(context, gateway, trace)
    try:
        await broker.discover()
    except (OSError, RuntimeError, ValueError):
        context.discovered_tools = frozenset()

    coordinator = "coordinator"
    entity = EntityOrderAgent(context, broker)
    _task_assigned(trace, case_id, coordinator, entity.name, "resolve_entities")
    entity_message = A2AMessage.create(
        case_id=case_id,
        sender=coordinator,
        recipient=entity.name,
        task="resolve_entities",
        payload={"candidate_order_ids": _candidate_order_ids(case)},
    )
    entity_result = await entity.run(entity_message)
    entity_handoff = A2AMessage.create(
        case_id=case_id,
        sender=entity.name,
        recipient=coordinator,
        task="entity_resolution_complete",
        payload={"status": entity_result["entity_resolution"]["status"]},
        normalized_data={
            "resolved_order_ids": entity_result["entity_resolution"]["resolved_order_ids"],
            "item_ids": entity_result.get("item_ids", []),
            "seller_ids": entity_result.get("seller_ids", []),
            "product_ids": entity_result.get("product_ids", []),
            "customer_id_candidates": entity_result.get("customer_id_candidates", []),
        },
        evidence_refs=_refs_from_result(entity_result),
    )
    _handoff(
        trace,
        case_id,
        entity.name,
        coordinator,
        "entity_resolution_complete",
        list(entity_handoff.evidence_refs),
    )

    order_ids = (
        entity_result["entity_resolution"]["resolved_order_ids"]
        if entity_result["entity_resolution"]["status"] == "resolved"
        else []
    )
    policy_agent = PolicyAgent(context, broker)
    specialist_specs: list[tuple[str, Agent, str]] = [
        ("customer_investigation", CustomerAgent(context, broker), "customer-agent"),
        ("shipment_investigation", ShipmentAgent(context, broker), "shipment-agent"),
        ("payment_investigation", PaymentRefundAgent(context, broker), "payment-refund-agent"),
        ("policy_check", policy_agent, "policy-agent"),
    ]
    messages: list[A2AMessage] = []
    for task, _agent, recipient in specialist_specs:
        _task_assigned(trace, case_id, coordinator, recipient, task)
        payload: dict[str, Any] = {"resolved_order_ids": order_ids}
        if recipient == "customer-agent":
            payload.update(
                {
                    "customer_id_candidates": entity_result.get("customer_id_candidates", []),
                    "customer_unique_id_hint": case.get("customer_unique_id_hint"),
                }
            )
        messages.append(
            A2AMessage.create(
                case_id=case_id,
                sender=coordinator,
                recipient=recipient,
                task=task,
                payload=payload,
                normalized_data=entity_handoff.normalized_data,
                evidence_refs=list(entity_handoff.evidence_refs),
            )
        )

    results = await asyncio.gather(
        *(
            agent.run(message)
            for (_, agent, _), message in zip(specialist_specs, messages, strict=True)
        ),
        return_exceptions=True,
    )
    for (task, _agent, recipient), result in zip(specialist_specs, results, strict=True):
        if isinstance(result, BaseException):
            context.findings[recipient] = {"evidence": [], "evidence_refs": [], "error": task}

    policy_message = A2AMessage.create(
        case_id=case_id,
        sender=coordinator,
        recipient=policy_agent.name,
        task="policy_decision",
        payload={"resolved_order_ids": order_ids},
        normalized_data={"evidence_refs": _all_evidence_refs(context)},
        evidence_refs=_bounded_unique(_all_evidence_refs(context)),
    )
    policy_decision = policy_agent.decide(policy_message)
    trace.emit(
        case_id=case_id,
        event_type="policy_decided",
        actor="policy-agent",
        decision_code=policy_decision["decision_code"],
        evidence_refs=_bounded_unique(policy_decision.get("evidence_refs", [])),
    )

    all_specialist_refs = _all_evidence_refs(context)
    _handoff(
        trace,
        case_id,
        coordinator,
        "conflict-resolver",
        "specialist_results_complete",
        all_specialist_refs,
    )
    conflict_agent = ConflictResolver(context, broker)
    conflict_message = A2AMessage.create(
        case_id=case_id,
        sender=coordinator,
        recipient=conflict_agent.name,
        task="resolve_conflicts",
        payload={"specialist_names": [recipient for _, _, recipient in specialist_specs]},
        normalized_data={"evidence_refs": all_specialist_refs, "policy_decision": policy_decision},
        evidence_refs=all_specialist_refs,
    )
    conflict_result = await conflict_agent.run(conflict_message)
    conflict_refs = _refs_from_result(conflict_result)
    _handoff(
        trace,
        case_id,
        conflict_agent.name,
        "verifier-agent",
        "verify_case",
        _bounded_unique([*all_specialist_refs, *conflict_refs]),
    )
    verifier = VerifierAgent(context, broker)
    verifier_message = A2AMessage.create(
        case_id=case_id,
        sender=conflict_agent.name,
        recipient=verifier.name,
        task="verify_case",
        payload={"conflict_count": len(conflict_result.get("data_conflicts", []))},
        normalized_data={"data_conflicts": conflict_result.get("data_conflicts", [])},
        evidence_refs=_bounded_unique([*all_specialist_refs, *conflict_refs]),
    )
    output = await verifier.run(verifier_message)
    trace.emit(
        case_id=case_id,
        event_type="verification_completed",
        actor=verifier.name,
        decision_code=(
            "passed" if not context.findings.get("verifier-agent", {}).get("issues") else "degraded"
        ),
        evidence_refs=_bounded_unique(output.get("evidence_refs", [])),
    )
    return output


def _task_assigned(trace: TraceWriter, case_id: str, actor: str, target: str, task: str) -> None:
    trace.emit(
        case_id=case_id,
        event_type="task_assigned",
        actor=actor,
        target=target,
        attributes={"task": task},
    )


def _handoff(
    trace: TraceWriter,
    case_id: str,
    actor: str,
    target: str,
    task: str,
    evidence_refs: list[str],
) -> None:
    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor=actor,
        target=target,
        evidence_refs=_bounded_unique(evidence_refs),
        attributes={"task": task},
    )


def _candidate_order_ids(case: dict[str, Any]) -> list[str]:
    values: list[str] = []
    request = case.get("customer_request")
    claimed = request.get("claimed_order_id") if isinstance(request, dict) else None
    if isinstance(claimed, str) and claimed:
        values.append(claimed)
    candidates = case.get("candidate_order_ids", [])
    if isinstance(candidates, list):
        values.extend(value for value in candidates if isinstance(value, str) and value)
    return _bounded_unique(values)


def _candidate_order_ids_from_message(message: A2AMessage) -> list[str]:
    values = message.payload.get("candidate_order_ids", [])
    return _bounded_unique(value for value in values if isinstance(value, str) and value)


def _order_ids_from_message(message: A2AMessage) -> list[str]:
    values = message.payload.get("resolved_order_ids", [])
    return _bounded_unique(value for value in values if isinstance(value, str))


def _is_retryable_exception(error: BaseException) -> bool:
    if isinstance(error, (TimeoutError, OSError, ConnectionError)):
        return True
    error_name = type(error).__name__.lower()
    if "timeout" in error_name or "connection" in error_name:
        return True
    if not isinstance(error, RuntimeError):
        return False
    message = str(error).lower()
    non_retryable_markers = (
        "authorization",
        "unauthorized",
        "forbidden",
        "permission",
        "case_id",
        "case mismatch",
        "wrong case",
        "invalid case",
        "invalid argument",
        "bad request",
        "validation",
    )
    return not any(marker in message for marker in non_retryable_markers)


def _walk(value: Any) -> list[dict[str, Any]]:
    """Walk object/list evidence payloads at any depth, preserving source order."""
    records: list[dict[str, Any]] = []
    if isinstance(value, dict):
        records.append(value)
        for nested in value.values():
            records.extend(_walk(nested))
    elif isinstance(value, list):
        for nested in value:
            records.extend(_walk(nested))
    return records


def _record_value(record: dict[str, Any], key: str) -> Any:
    """Read snake_case/camelCase variants without assuming MCP key spelling."""
    if key in record:
        return record[key]
    normalized = "".join(character for character in key.casefold() if character.isalnum())
    for candidate, value in record.items():
        if (
            isinstance(candidate, str)
            and "".join(character for character in candidate.casefold() if character.isalnum())
            == normalized
        ):
            return value
    return None


def _first_value(value: Any, keys: tuple[str, ...]) -> str | None:
    for record in _walk(value):
        for key in keys:
            candidate = _record_value(record, key)
            if isinstance(candidate, (str, int)) and str(candidate):
                return str(candidate)
    return None


def _first_value_from_evidence(
    evidence_items: list[dict[str, Any]], keys: tuple[str, ...]
) -> str | None:
    for evidence in evidence_items:
        value = _first_value(evidence.get("data"), keys)
        if value is not None:
            return value
    return None


def _all_values(evidence_items: list[dict[str, Any]], keys: tuple[str, ...]) -> list[Any]:
    values: list[Any] = []
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            for key in keys:
                value = _record_value(record, key)
                if value is not None and value not in values:
                    values.append(value)
    return values


def _ids_from_evidence(evidence: dict[str, Any] | None, keys: tuple[str, ...]) -> list[str]:
    if evidence is None:
        return []
    values: list[str] = []
    for record in _walk(evidence.get("data")):
        for key in keys:
            value = _record_value(record, key)
            if isinstance(value, (str, int)) and str(value):
                values.append(str(value))
    return _bounded_unique(values)


def _refs(*values: dict[str, Any] | None | list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for value in values:
        if isinstance(value, list):
            refs.extend(_refs(*value))
        elif isinstance(value, dict) and isinstance(value.get("evidence_ref"), str):
            refs.append(value["evidence_ref"])
    return _bounded_unique(refs)


def _refs_from_result(result: dict[str, Any]) -> list[str]:
    refs = result.get("evidence_refs", [])
    return _bounded_unique(refs if isinstance(refs, list) else [])


def _all_evidence_refs(context: CaseContext) -> list[str]:
    return _bounded_unique(context.evidence, 30)


def _bounded_unique(values: Any, limit: int = 20) -> list[str]:
    result: list[str] = []
    for value in values:
        if isinstance(value, str) and value not in result:
            result.append(value)
        if len(result) >= limit:
            break
    return result


def _normalize_conflict(raw: dict[str, Any], _source_name: str) -> dict[str, Any] | None:
    field = _record_value(raw, "field")
    sources = _record_value(raw, "sources")
    selected = _record_value(raw, "selected_source")
    resolution = _record_value(raw, "resolution_code")
    if not isinstance(field, str) or not field:
        return None
    if not isinstance(sources, list):
        return None
    sources = _bounded_unique(
        [source for source in sources if isinstance(source, str) and source], 5
    )
    if len(sources) < 2:
        return None
    if not isinstance(selected, (str, type(None))):
        selected = None
    elif isinstance(selected, str):
        selected = selected[:80]
    if not isinstance(resolution, str) or not resolution:
        return None
    return {
        "field": field[:100],
        "sources": sources,
        "selected_source": selected,
        "resolution_code": resolution[:80],
    }


def _analysis_value(
    evidence_items: list[dict[str, Any]], keys: tuple[str, ...], allowed: set[str]
) -> str | None:
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            for key in keys:
                value = _record_value(record, key)
                if isinstance(value, str):
                    normalized = value.strip().lower()
                    if normalized in allowed:
                        return normalized
    return None


def _number_value(evidence_items: list[dict[str, Any]], keys: tuple[str, ...]) -> float | None:
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            for key in keys:
                value = _record_value(record, key)
                decimal = _as_decimal(value)
                if decimal is not None:
                    return float(decimal)
    return None


def _build_public_output(context: CaseContext) -> dict[str, Any]:
    entity = context.findings.get("entity-order-agent", {})
    customer = context.findings.get("customer-agent", {})
    shipment = context.findings.get("shipment-agent", {})
    payment = context.findings.get("payment-refund-agent", {})
    policy_finding = context.findings.get("policy-agent", {})
    conflict = context.findings.get("conflict-resolver", {})
    policy_decision = policy_finding.get("decision") or PolicyEngine(context).evaluate()
    order_ids = _bounded_unique(entity.get("entity_resolution", {}).get("resolved_order_ids", []))
    item_ids = _bounded_unique(entity.get("item_ids", []))
    seller_ids = _bounded_unique(entity.get("seller_ids", []))
    payment_evidence = payment.get("evidence", [])
    shipment_evidence = shipment.get("evidence", [])
    payment_refs = _ids_from_evidence_list(payment_evidence, PAYMENT_ID_KEYS)
    shipment_ids = _ids_from_evidence_list(shipment_evidence, SHIPMENT_ID_KEYS)

    shipment_analysis = policy_decision.get("shipment_analysis") or _finding_analysis(
        context, "shipment-agent", _normalize_shipment_evidence(shipment_evidence)
    )
    payment_analysis = policy_decision.get("payment_analysis") or _finding_analysis(
        context, "payment-refund-agent", _normalize_payment_evidence(payment_evidence)
    )
    shipment_verdict = shipment_analysis.get("verdict", "insufficient_evidence")
    payment_verdict = payment_analysis.get("verdict", "insufficient_evidence")
    public_payment_verdict = (
        "reconciled" if payment_verdict == "valid_split_payment" else payment_verdict
    )
    all_refs = _bounded_unique(_all_evidence_refs(context), 30)
    financial = policy_decision.get("financial_resolution") or _financial_resolution(
        payment_evidence, _finding_evidence(context, "policy-agent")
    )
    decision_conflicts = policy_decision.get("data_conflicts", [])
    data_conflicts = conflict.get("data_conflicts", decision_conflicts)
    claims = policy_decision.get("claim_assessments") or _claim_assessments(
        context,
        all_refs,
        shipment_verdict=shipment_verdict,
        payment_verdict=payment_verdict,
        financial=financial,
        conflicts=bool(data_conflicts),
        primary_issue=policy_decision.get("primary_issue"),
    )

    output: dict[str, Any] = {
        "schema_version": "day09-l3b-output-v2",
        "case_id": context.case_id,
        "assessment": {
            "primary_issue": policy_decision.get("primary_issue", "insufficient_evidence"),
            "secondary_issues": policy_decision.get(
                "secondary_issues",
                _secondary_issues(shipment_verdict, payment_verdict, "insufficient_evidence"),
            ),
            "case_status": policy_decision.get("case_status", "needs_investigation"),
            "confidence": _clamp_confidence(policy_decision.get("confidence", 0.0)),
        },
        "affected_entities": {
            "order_ids": order_ids,
            "item_ids": item_ids,
            "seller_ids": seller_ids,
            "payment_references": payment_refs,
            "shipment_ids": shipment_ids,
        },
        "entity_resolution": entity.get(
            "entity_resolution",
            {
                "status": "not_found",
                "resolved_order_ids": [],
                "rejected_candidates": [],
                "confidence": 0.0,
            },
        ),
        "customer_context": customer.get(
            "customer_context", {"customer_unique_id": None, "related_order_ids": []}
        ),
        "shipment_analysis": {
            "verdict": shipment_verdict,
            "late_seller_ids": _bounded_unique(
                shipment_analysis.get("late_seller_ids", [])
                if shipment_verdict == "seller_delay"
                else []
            ),
            "timeline_complete": shipment_analysis.get("timeline_complete") is True,
        },
        "payment_analysis": {
            "verdict": public_payment_verdict,
            "captured_total_brl": _public_decimal(
                _as_decimal(payment_analysis.get("captured_total_brl"))
            ),
            "refunded_total_brl": _public_decimal(
                _as_decimal(payment_analysis.get("refunded_total_brl"))
            ),
            "refundable_total_brl": _public_decimal(
                _as_decimal(payment_analysis.get("refundable_total_brl"))
            ),
        },
        "root_cause_analysis": policy_decision.get(
            "root_cause_analysis", _root_cause(shipment_verdict, payment_verdict, seller_ids)
        ),
        "evidence_refs": all_refs,
        "data_conflicts": data_conflicts,
        "financial_resolution": financial,
        "resolution_actions": policy_decision.get("resolution_actions", []),
    }
    if claims:
        output["claim_assessments"] = claims
    return output


def _ids_from_evidence_list(
    evidence_items: list[dict[str, Any]], keys: tuple[str, ...]
) -> list[str]:
    values: list[str] = []
    for evidence in evidence_items:
        values.extend(_ids_from_evidence(evidence, keys))
    return _bounded_unique(values)


def _bool_value(evidence_items: list[dict[str, Any]], keys: tuple[str, ...]) -> bool | None:
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            for key in keys:
                value = _record_value(record, key)
                if isinstance(value, bool):
                    return value
    return None


def _primary_issue(
    shipment_verdict: str, payment_verdict: str, payment_evidence: list[dict[str, Any]]
) -> str:
    shipment_map = {
        "seller_delay": "late_delivery_seller",
        "logistics_delay": "late_delivery_logistics",
    }
    payment_map = {
        "capture_mismatch": "payment_mismatch",
        "duplicate_capture": "duplicate_charge",
        "refund_pending": "refund_pending",
        "refund_failed": "refund_failed",
    }
    if shipment_verdict in shipment_map:
        return shipment_map[shipment_verdict]
    if payment_verdict in payment_map:
        return payment_map[payment_verdict]
    for evidence in payment_evidence:
        value = _first_value(evidence.get("data"), ("primary_issue",))
        if value in {
            "canceled_order_paid",
            "unavailable_order_paid",
            "valid_split_payment",
            "unsupported_claim",
        }:
            return value
    return "insufficient_evidence"


def _secondary_issues(shipment_verdict: str, payment_verdict: str, primary: str) -> list[str]:
    candidates: list[str] = []
    if shipment_verdict == "seller_delay" and primary != "late_delivery_seller":
        candidates.append("late_delivery_seller")
    if shipment_verdict == "logistics_delay" and primary != "late_delivery_logistics":
        candidates.append("late_delivery_logistics")
    if payment_verdict == "capture_mismatch" and primary != "payment_mismatch":
        candidates.append("payment_mismatch")
    if payment_verdict == "duplicate_capture" and primary != "duplicate_charge":
        candidates.append("duplicate_charge")
    return _bounded_unique(candidates, 10)


def _root_cause(
    shipment_verdict: str, payment_verdict: str, seller_ids: list[str]
) -> dict[str, Any]:
    causes: list[dict[str, Any]] = []
    parties: list[dict[str, Any]] = []
    if shipment_verdict == "seller_delay":
        causes.append({"cause_code": "SELLER_DELAY", "rank": 1})
        parties.extend({"party_type": "seller", "party_id": value} for value in seller_ids[:5])
    elif shipment_verdict == "logistics_delay":
        causes.append({"cause_code": "LOGISTICS_DELAY", "rank": 1})
        parties.append({"party_type": "logistics_provider", "party_id": None})
    elif payment_verdict == "capture_mismatch":
        causes.append({"cause_code": "PAYMENT_CAPTURE_MISMATCH", "rank": 1})
        parties.append({"party_type": "payment_provider", "party_id": None})
    elif payment_verdict == "duplicate_capture":
        causes.append({"cause_code": "DUPLICATE_CAPTURE", "rank": 1})
        parties.append({"party_type": "payment_provider", "party_id": None})
    return {"ranked_causes": causes, "responsible_parties": parties}


def _explicit_actions(context: CaseContext) -> list[str]:
    actions: list[str] = []
    for finding in context.findings.values():
        for evidence in finding.get("evidence", []):
            value = evidence.get("data") if isinstance(evidence, dict) else None
            if not isinstance(value, dict) or not isinstance(value.get("resolution_actions"), list):
                continue
            actions.extend(
                item[:80] for item in value["resolution_actions"] if isinstance(item, str) and item
            )
    return _bounded_unique(actions, 8)


def _legacy_financial_resolution(payment_evidence: list[dict[str, Any]]) -> dict[str, Any]:
    recommended = _number_value(payment_evidence, ("recommended_refund_brl",))
    lines: list[dict[str, Any]] = []
    for evidence in payment_evidence:
        data = evidence.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("refund_lines"), list):
            continue
        for line in data["refund_lines"]:
            if not isinstance(line, dict):
                continue
            reason = line.get("reason_code")
            amount = line.get("amount_brl")
            entity_id = line.get("entity_id")
            if (
                isinstance(reason, str)
                and isinstance(amount, (int, float))
                and amount >= 0
                and isinstance(entity_id, (str, type(None)))
            ):
                lines.append(
                    {"reason_code": reason[:80], "amount_brl": amount, "entity_id": entity_id}
                )
    if recommended is None:
        recommended = sum(float(line["amount_brl"]) for line in lines)
    return {
        "currency": "BRL",
        "recommended_refund_brl": max(0.0, recommended),
        "refund_lines": lines[:10],
    }


def _legacy_claim_assessments(context: CaseContext, all_refs: list[str]) -> list[dict[str, Any]]:
    request = context.case.get("customer_request", {})
    claims = request.get("claims", []) if isinstance(request, dict) else []
    if not isinstance(claims, list):
        return []
    result: list[dict[str, Any]] = []
    for claim in claims[:5]:
        if not isinstance(claim, dict) or not isinstance(claim.get("claim_id"), str):
            continue
        result.append(
            {
                "claim_id": claim["claim_id"][:64],
                "verdict": "insufficient_evidence",
                "confidence": 0.0,
                "evidence_refs": all_refs[:3],
            }
        )
    return result


def _finding_evidence(context: CaseContext, agent: str) -> list[dict[str, Any]]:
    finding = context.findings.get(agent, {})
    return [item for item in finding.get("evidence", []) if isinstance(item, dict)]


def _specialist_errors(context: CaseContext) -> bool:
    return any(
        isinstance(finding.get("error"), str)
        for name, finding in context.findings.items()
        if name not in {"conflict-resolver", "verifier-agent"}
    )


def _clamp_confidence(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if numeric != numeric:
        return 0.0
    return round(max(0.0, min(1.0, numeric)), 4)


def _decimal_value(evidence_items: list[dict[str, Any]], keys: tuple[str, ...]) -> Decimal | None:
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            for key in keys:
                value = _record_value(record, key)
                if isinstance(value, bool) or value is None:
                    continue
                try:
                    decimal = Decimal(str(value))
                except (InvalidOperation, ValueError):
                    continue
                if decimal.is_finite() and decimal >= 0:
                    return decimal.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    return None


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not decimal.is_finite() or decimal < 0:
        return None
    return decimal.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    except ValueError:
        return None


def _lower_values(records: list[dict[str, Any]], keys: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    for record in records:
        for key in keys:
            value = _record_value(record, key)
            if isinstance(value, str) and value:
                values.append(value.strip().lower())
    return values


def _data_records(evidence_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return nested data records without assuming one MCP response shape."""
    records: list[dict[str, Any]] = []
    for evidence in evidence_items:
        if not isinstance(evidence, dict):
            continue
        records.extend(_walk(evidence.get("data")))
    return records


def _normalize_shipment_evidence(evidence_items: list[dict[str, Any]]) -> dict[str, Any]:
    """Normalize the order-scoped shipment timeline returned by MCP.

    The gateway returns a summary object with timestamps, `shipping_limits`, and
    event rows; it does not return a precomputed `verdict` field.  This helper
    only classifies a delay when the relevant timestamps are present.
    """
    records = _data_records(evidence_items)
    explicit = _analysis_value(
        evidence_items,
        ("verdict", "shipment_verdict"),
        {
            "on_time",
            "seller_delay",
            "logistics_delay",
            "lost",
            "returned",
            "conflicting",
            "insufficient_evidence",
        },
    )
    carrier_at = next(
        (
            value
            for record in records
            for value in (_timestamp(_record_value(record, "delivered_carrier_at")),)
            if value
        ),
        None,
    )
    delivered_at = next(
        (
            value
            for record in records
            for value in (_timestamp(_record_value(record, "delivered_customer_at")),)
            if value
        ),
        None,
    )
    estimated_at = next(
        (
            value
            for record in records
            for value in (_timestamp(_record_value(record, "estimated_delivery_at")),)
            if value
        ),
        None,
    )
    limits: list[tuple[datetime, str | None]] = []
    for record in records:
        limit = _timestamp(_record_value(record, "shipping_limit_at"))
        if limit is not None:
            seller_id = _record_value(record, "seller_id")
            limits.append((limit, seller_id if isinstance(seller_id, str) else None))
    statuses = _lower_values(records, ("order_status", "status", "event_type"))
    timeline_complete = bool(carrier_at and delivered_at and estimated_at and limits)
    late_seller_ids: list[str] = []
    if carrier_at is not None:
        late_seller_ids = _bounded_unique(
            [
                seller_id
                for limit, seller_id in limits
                if carrier_at > limit and seller_id is not None
            ]
        )
    if explicit is not None:
        verdict = explicit
    elif any("returned" in status for status in statuses):
        verdict = "returned"
    elif any("lost" in status for status in statuses):
        verdict = "lost"
    elif late_seller_ids:
        verdict = "seller_delay"
    elif (
        timeline_complete
        and delivered_at is not None
        and estimated_at is not None
        and delivered_at > estimated_at
    ):
        verdict = "logistics_delay"
    elif timeline_complete:
        verdict = "on_time"
    else:
        verdict = "insufficient_evidence"
    return {
        "verdict": verdict,
        "late_seller_ids": late_seller_ids if verdict == "seller_delay" else [],
        "timeline_complete": timeline_complete,
    }


def _normalize_payment_evidence(evidence_items: list[dict[str, Any]]) -> dict[str, Any]:
    """Normalize payment/refund list and timeline evidence without fabricating ids."""
    records = _data_records(evidence_items)
    explicit = _analysis_value(
        evidence_items,
        ("verdict", "payment_verdict", "refund_verdict"),
        {
            "reconciled",
            "capture_mismatch",
            "duplicate_capture",
            "refund_pending",
            "refund_failed",
            "refunded",
            "valid_split_payment",
            "insufficient_evidence",
        },
    )
    payment_records = [
        record for record in records if _record_value(record, "payment_value") is not None
    ]
    unique_payments: list[dict[str, Any]] = []
    fingerprints: set[tuple[str, str, str, str]] = set()
    for record in payment_records:
        amount = _as_decimal(_record_value(record, "payment_value"))
        if amount is None:
            continue
        fingerprint = (
            str(_record_value(record, "order_id") or ""),
            str(_record_value(record, "payment_sequential") or ""),
            str(_record_value(record, "payment_type") or ""),
            str(amount),
        )
        if fingerprint not in fingerprints:
            fingerprints.add(fingerprint)
            unique_payments.append(record)
    captured = sum(
        (
            _as_decimal(_record_value(record, "payment_value")) or Decimal("0")
            for record in unique_payments
        ),
        Decimal("0"),
    ).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    event_records = [
        record
        for record in records
        if _record_value(record, "event_type") is not None
        or _record_value(record, "type") is not None
        or _record_value(record, "refund_status") is not None
    ]
    event_text = " ".join(
        _lower_values(event_records, ("event_type", "type", "status", "refund_status"))
    )
    capture_amounts = []
    for record in event_records:
        event_type = str(
            _record_value(record, "event_type") or _record_value(record, "type") or ""
        ).lower()
        status = str(_record_value(record, "status") or "").lower()
        if any(token in event_type for token in ("capture", "paid")) or status in {
            "captured",
            "paid",
        }:
            capture_amounts.append(
                _as_decimal(
                    _record_value(record, "amount_brl")
                    or _record_value(record, "capture_amount_brl")
                    or _record_value(record, "amount")
                )
            )
    captured_events = sum((value for value in capture_amounts if value is not None), Decimal("0"))
    refund_records = [
        record
        for record in event_records
        if "refund"
        in str(_record_value(record, "event_type") or _record_value(record, "type") or "").lower()
    ]
    refunded_amounts = []
    for record in refund_records:
        status = str(
            _record_value(record, "status") or _record_value(record, "refund_status") or ""
        ).lower()
        if not any(token in status for token in ("pending", "failed")):
            refunded_amounts.append(
                _as_decimal(
                    _record_value(record, "amount_brl")
                    or _record_value(record, "refund_amount_brl")
                    or _record_value(record, "amount")
                )
            )
    refunded = sum((value for value in refunded_amounts if value is not None), Decimal("0"))
    refunded = refunded.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    sequence_values = [_record_value(record, "payment_sequential") for record in unique_payments]
    if explicit is not None:
        verdict = explicit
    elif "refund_failed" in event_text or ("refund" in event_text and "failed" in event_text):
        verdict = "refund_failed"
    elif "refund_pending" in event_text or ("refund" in event_text and "pending" in event_text):
        verdict = "refund_pending"
    # A repeated payment sequence alone is not proof of a duplicate capture:
    # processors can use the same ordinal for distinct payment methods.  Require
    # an explicit duplicate marker from the payment timeline.
    elif "duplicate" in event_text:
        verdict = "duplicate_capture"
    elif "mismatch" in event_text or (captured_events > 0 and captured_events != captured):
        verdict = "capture_mismatch"
    elif len(unique_payments) > 1 and (
        len(set(str(value) for value in sequence_values)) > 1
        or len({str(_record_value(record, "payment_type") or "") for record in unique_payments}) > 1
    ):
        verdict = "valid_split_payment"
    elif unique_payments:
        verdict = "reconciled"
    else:
        verdict = "insufficient_evidence"
    return {
        "verdict": verdict,
        "captured_total_brl": captured
        if unique_payments
        else (
            captured_events
            if any(value is not None for value in capture_amounts)
            else _decimal_value(evidence_items, ("captured_total_brl", "captured_total"))
        ),
        "refunded_total_brl": (
            refunded
            if refund_records
            else _decimal_value(evidence_items, ("refunded_total_brl", "refunded_total"))
        ),
        "refundable_total_brl": (
            max(
                Decimal("0"),
                (
                    captured
                    if unique_payments
                    else captured_events
                    if any(value is not None for value in capture_amounts)
                    else Decimal("0")
                )
                - refunded,
            ).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)
            if unique_payments or any(value is not None for value in capture_amounts)
            else _decimal_value(evidence_items, ("refundable_total_brl", "refundable_total"))
        ),
    }


def _finding_analysis(context: CaseContext, agent: str, fallback: dict[str, Any]) -> dict[str, Any]:
    analysis = context.findings.get(agent, {}).get("analysis")
    return analysis if isinstance(analysis, dict) else fallback


def _public_decimal(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP))


def _refund_lines_from_evidence(
    evidence_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            raw_lines = _record_value(record, "refund_lines")
            if not isinstance(raw_lines, list):
                continue
            for raw_line in raw_lines:
                if not isinstance(raw_line, dict):
                    continue
                reason = _record_value(raw_line, "reason_code")
                entity_id = _record_value(raw_line, "entity_id")
                amount = _record_value(raw_line, "amount_brl")
                if not isinstance(reason, str) or not isinstance(entity_id, (str, type(None))):
                    continue
                try:
                    decimal_amount = Decimal(str(amount))
                except (InvalidOperation, ValueError):
                    continue
                if not decimal_amount.is_finite() or decimal_amount < 0:
                    continue
                line = {
                    "reason_code": reason[:80],
                    "amount_brl": decimal_amount.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP),
                    "entity_id": entity_id[:128] if isinstance(entity_id, str) else None,
                }
                if line not in lines:
                    lines.append(line)
    return lines[:10]


def _financial_resolution(
    payment_evidence: list[dict[str, Any]],
    policy_evidence: list[dict[str, Any]] | None = None,
    *,
    primary_issue: str | None = None,
    payment_analysis: dict[str, Any] | None = None,
    policy_rule: dict[str, Any] | None = None,
    entity_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Build a bounded financial recommendation from case-local evidence only."""
    policy_evidence = policy_evidence or []
    payment_analysis = payment_analysis or _normalize_payment_evidence(payment_evidence)
    captured = _as_decimal(payment_analysis.get("captured_total_brl"))
    refunded = _as_decimal(payment_analysis.get("refunded_total_brl"))
    refundable = _as_decimal(payment_analysis.get("refundable_total_brl"))
    policy_rule = policy_rule or _policy_rule(policy_evidence, primary_issue or "")
    recommended = (
        _as_decimal(
            policy_rule.get(
                "refund_brl",
                policy_rule.get("recommended_refund_brl", policy_rule.get("refund_amount_brl")),
            )
        )
        if policy_rule
        else None
    )
    if recommended is None and primary_issue is None:
        # Legacy/direct unit callers may provide an already-selected policy
        # decision. Production calls always pass the selected issue/rule.
        recommended = _decimal_value(
            policy_evidence, ("recommended_refund_brl", "recommended_refund", "refund_amount_brl")
        )
    # Backward-compatible handling for a response that actually supplies
    # structured refund lines, but never select a value from an unrelated rule.
    lines = _refund_lines_from_evidence(payment_evidence)
    line_total = sum((line["amount_brl"] for line in lines), Decimal("0")).quantize(
        MONEY_QUANTUM, rounding=ROUND_HALF_UP
    )
    if lines:
        if recommended is None:
            recommended = line_total
        elif recommended != line_total:
            return {"currency": "BRL", "recommended_refund_brl": 0.0, "refund_lines": []}
    elif recommended is None:
        recommended = Decimal("0")

    # A policy amount is a recommendation, not evidence that a refund has
    # already happened.  It is usable only with a known captured/refundable
    # amount and is always capped by the order-local payment evidence.
    if not lines and recommended > 0:
        cap_for_line = refundable
        if cap_for_line is None and captured is not None:
            cap_for_line = max(Decimal("0"), captured - (refunded or Decimal("0")))
        if cap_for_line is None:
            recommended = Decimal("0")
        else:
            recommended = min(recommended, cap_for_line)
            if recommended > 0:
                lines = [
                    {
                        "reason_code": (primary_issue or "policy_refund")[:80],
                        "amount_brl": recommended,
                        "entity_id": (entity_ids or [None])[0],
                    }
                ]

    if captured is not None and refunded is not None:
        captured_remaining = max(Decimal("0"), captured - refunded)
    else:
        captured_remaining = None
    cap = refundable if refundable is not None else captured_remaining
    if cap is not None and recommended > cap:
        return {"currency": "BRL", "recommended_refund_brl": 0.0, "refund_lines": []}
    if recommended < 0:
        return {"currency": "BRL", "recommended_refund_brl": 0.0, "refund_lines": []}
    return {
        "currency": "BRL",
        "recommended_refund_brl": _public_decimal(recommended) or 0.0,
        "refund_lines": [
            {
                "reason_code": line["reason_code"],
                "amount_brl": _public_decimal(line["amount_brl"]) or 0.0,
                "entity_id": line["entity_id"],
            }
            for line in lines
        ],
    }


def _scalar_value(evidence_items: list[dict[str, Any]], keys: tuple[str, ...]) -> Any:
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            for key in keys:
                value = _record_value(record, key)
                if value is not None:
                    return value
    return None


def _bool_evidence_value(
    evidence_items: list[dict[str, Any]], keys: tuple[str, ...]
) -> bool | None:
    value = _scalar_value(evidence_items, keys)
    return value if isinstance(value, bool) else None


def _finding_refs(context: CaseContext, agent: str) -> list[str]:
    refs = context.findings.get(agent, {}).get("evidence_refs", [])
    return _bounded_unique(refs if isinstance(refs, list) else [])


def _policy_explicit_value(
    evidence_items: list[dict[str, Any]], keys: tuple[str, ...], allowed: set[str]
) -> str | None:
    for evidence in evidence_items:
        for record in _walk(evidence.get("data")):
            for key in keys:
                value = _record_value(record, key)
                if isinstance(value, str) and value.strip().lower() in allowed:
                    return value.strip().lower()
    return None


def _policy_rule(evidence_items: list[dict[str, Any]], primary_issue: str) -> dict[str, Any]:
    """Select only the rule for the chosen issue from MCP business policy."""
    if primary_issue not in PRIMARY_ISSUES:
        return {}
    for evidence in evidence_items:
        data = evidence.get("data") if isinstance(evidence, dict) else None
        for record in _walk(data):
            rules = next(
                (
                    _record_value(record, key)
                    for key in ("rules", "policy_rules", "issue_rules", "decision_rules")
                    if _record_value(record, key) is not None
                ),
                None,
            )
            if isinstance(rules, dict) and isinstance(rules.get(primary_issue), dict):
                return rules[primary_issue]
            if isinstance(rules, list):
                for rule in rules:
                    if not isinstance(rule, dict):
                        continue
                    issue = _record_value(rule, "primary_issue") or _record_value(rule, "issue")
                    if issue == primary_issue:
                        return rule
    return {}


def _claims_contradicted_by_evidence(
    context: CaseContext, shipment_verdict: Any, payment_verdict: Any
) -> bool:
    request = context.case.get("customer_request")
    claims = request.get("claims", []) if isinstance(request, dict) else []
    if not isinstance(claims, list) or not claims:
        return False
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        topic = str(
            claim.get("topic", claim.get("claim_type", claim.get("category", "")))
        ).casefold()
        if topic == "late_delivery_logistics" and shipment_verdict in {"seller_delay", "on_time"}:
            return True
        if topic == "late_delivery_seller" and shipment_verdict in {"logistics_delay", "on_time"}:
            return True
        if topic in {"payment_mismatch", "duplicate_charge"} and payment_verdict == "reconciled":
            return True
        if topic in {"refund_pending", "refund_failed"} and payment_verdict == "refunded":
            return True
        if (
            topic == "requested_full_refund"
            and _bool_evidence_value(
                _finding_evidence(context, "policy-agent"),
                ("refund_eligible", "full_refund_eligible"),
            )
            is False
        ):
            return True
    return False


def _choose_primary_issue(
    policy_evidence: list[dict[str, Any]],
    shipment_evidence: list[dict[str, Any]],
    payment_evidence: list[dict[str, Any]],
    *,
    context: CaseContext | None = None,
    shipment_analysis: dict[str, Any] | None = None,
    payment_analysis: dict[str, Any] | None = None,
) -> str:
    if context is not None:
        resolution = context.findings.get("entity-order-agent", {}).get("entity_resolution", {})
        if resolution.get("status") != "resolved":
            return "insufficient_evidence"
    explicit_policy = _policy_explicit_value(
        policy_evidence,
        ("primary_issue", "recommended_primary_issue", "decision_primary_issue"),
        set(PRIMARY_ISSUES),
    )
    if explicit_policy is not None:
        return explicit_policy
    explicit_evidence = _policy_explicit_value(
        [*shipment_evidence, *payment_evidence], ("primary_issue",), set(PRIMARY_ISSUES)
    )
    if explicit_evidence is not None:
        return explicit_evidence
    shipment = (shipment_analysis or _normalize_shipment_evidence(shipment_evidence)).get("verdict")
    payment = (payment_analysis or _normalize_payment_evidence(payment_evidence)).get("verdict")
    payment_map = {
        "refund_failed": "refund_failed",
        "refund_pending": "refund_pending",
        "duplicate_capture": "duplicate_charge",
        "capture_mismatch": "payment_mismatch",
    }
    if payment in payment_map:
        return payment_map[payment]
    order_evidence = _finding_evidence(context, "entity-order-agent") if context else []
    order_statuses = _lower_values(_data_records(order_evidence), ("order_status", "status"))
    captured = _as_decimal(
        (payment_analysis or _normalize_payment_evidence(payment_evidence)).get(
            "captured_total_brl"
        )
    )
    if captured is not None and captured > 0:
        if any("cancel" in status for status in order_statuses):
            return "canceled_order_paid"
        if any("unavailable" in status for status in order_statuses):
            return "unavailable_order_paid"
    if shipment == "seller_delay":
        return "late_delivery_seller"
    if shipment == "logistics_delay":
        return "late_delivery_logistics"
    if (
        payment == "valid_split_payment"
        or _scalar_value(payment_evidence, ("split_payment_valid",)) is True
    ):
        return "valid_split_payment"
    if context is not None and _claims_contradicted_by_evidence(context, shipment, payment):
        return "unsupported_claim"
    return "insufficient_evidence"


def _supporting_refs(
    primary_issue: str,
    context: CaseContext,
    shipment_evidence: list[dict[str, Any]],
    payment_evidence: list[dict[str, Any]],
    policy_evidence: list[dict[str, Any]],
) -> list[str]:
    refs: list[str] = []
    if primary_issue.startswith("late_delivery"):
        refs.extend(_finding_refs(context, "shipment-agent"))
    if primary_issue in {
        "payment_mismatch",
        "duplicate_charge",
        "refund_pending",
        "refund_failed",
        "valid_split_payment",
    }:
        refs.extend(_finding_refs(context, "payment-refund-agent"))
    if primary_issue in {"canceled_order_paid", "unavailable_order_paid", "unsupported_claim"}:
        refs.extend(_finding_refs(context, "entity-order-agent"))
        refs.extend(_finding_refs(context, "payment-refund-agent"))
    if primary_issue == "unsupported_claim":
        refs.extend(_finding_refs(context, "shipment-agent"))
    # Policy was consumed to select (or reject) a business decision.  Include
    # its server-issued ref even for an evidence-insufficient result.
    refs.extend(_refs(*policy_evidence))
    return _bounded_unique(refs)


def _choose_case_status(
    primary_issue: str,
    entity: dict[str, Any],
    policy_evidence: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
    *,
    policy_rule: dict[str, Any] | None = None,
) -> str:
    entity_status = entity.get("entity_resolution", {}).get("status")
    if primary_issue == "insufficient_evidence" or entity_status != "resolved" or conflicts:
        return "needs_investigation"
    explicit = (policy_rule or {}).get("case_status")
    if explicit not in CASE_STATUSES:
        explicit = _policy_explicit_value(policy_evidence, ("case_status",), set(CASE_STATUSES))
    if explicit is not None:
        return explicit
    if primary_issue == "unsupported_claim":
        return "no_action"
    if not policy_evidence:
        return "needs_investigation"
    return "action_required"


def _policy_actions(
    primary_issue: str,
    status: str,
    policy_evidence: list[dict[str, Any]],
    financial: dict[str, Any],
    *,
    policy_rule: dict[str, Any] | None = None,
) -> list[str]:
    if status == "no_action":
        return []
    explicit: list[str] = []
    rule_action = (policy_rule or {}).get("recommended_action")
    if isinstance(rule_action, str) and rule_action:
        explicit.append(rule_action[:80])
    rule_actions = (policy_rule or {}).get("resolution_actions")
    if isinstance(rule_actions, list):
        explicit.extend(item[:80] for item in rule_actions if isinstance(item, str) and item)
    if status == "needs_investigation":
        return ["collect_additional_evidence"]
    if financial.get("recommended_refund_brl", 0) <= 0:
        explicit = [
            action
            for action in explicit
            if not any(token in action.casefold() for token in ("refund", "reimburse"))
        ]
    if explicit:
        return _bounded_unique(explicit, 8)
    action_by_issue = {
        "late_delivery_seller": "contact_seller",
        "late_delivery_logistics": "escalate_logistics",
        "payment_mismatch": "reconcile_payment",
        "duplicate_charge": "investigate_duplicate_charge",
        "refund_pending": "monitor_refund",
        "refund_failed": "escalate_refund_failure",
        "canceled_order_paid": "review_canceled_order_payment",
        "unavailable_order_paid": "review_unavailable_order_payment",
        "valid_split_payment": "close_payment_review",
        "unsupported_claim": "close_unsupported_claim",
        "insufficient_evidence": "collect_additional_evidence",
    }
    actions = [action_by_issue.get(primary_issue, "collect_additional_evidence")]
    if financial.get("recommended_refund_brl", 0) > 0:
        actions.append("process_refund_under_policy")
    return _bounded_unique(actions, 8)


def _responsible_parties(
    primary_issue: str,
    context: CaseContext,
    policy_evidence: list[dict[str, Any]],
    shipment_evidence: list[dict[str, Any]],
    payment_evidence: list[dict[str, Any]],
    *,
    policy_rule: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    explicit: list[dict[str, Any]] = []
    raw_parties = (policy_rule or {}).get("responsible_parties")
    if isinstance(raw_parties, list):
        for party in raw_parties:
            if not isinstance(party, dict):
                continue
            party_type = party.get("party_type")
            party_id = party.get("party_id")
            if party_type in RESPONSIBLE_PARTY_TYPES and isinstance(party_id, (str, type(None))):
                explicit.append({"party_type": party_type, "party_id": party_id})
    entity = context.findings.get("entity-order-agent", {})
    seller_ids = _bounded_unique(entity.get("seller_ids", []), 5)
    logistics_ids = _ids_from_evidence_list(
        shipment_evidence, ("logistics_provider_id", "carrier_id", "logistics_id")
    )
    payment_ids = _ids_from_evidence_list(
        payment_evidence, ("payment_provider_id", "provider_id", "gateway_id")
    )
    # Policy can authorize a party type but often leaves the id null.  Enrich
    # only seller ids which are proven by the resolved order's item evidence.
    if primary_issue == "late_delivery_seller":
        supported = [
            party
            for party in explicit
            if party["party_type"] == "seller"
            and (party["party_id"] is None or party["party_id"] in seller_ids)
        ]
        if supported:
            explicit = supported
        elif seller_ids:
            return [{"party_type": "seller", "party_id": value} for value in seller_ids]
        else:
            return [{"party_type": "unknown", "party_id": None}]
    elif primary_issue == "late_delivery_logistics":
        supported = [party for party in explicit if party["party_type"] == "logistics_provider"]
        if supported:
            explicit = supported
        elif logistics_ids:
            return [
                {"party_type": "logistics_provider", "party_id": value} for value in logistics_ids
            ]
        else:
            return [{"party_type": "unknown", "party_id": None}]
    elif primary_issue in {
        "payment_mismatch",
        "duplicate_charge",
        "refund_pending",
        "refund_failed",
    }:
        supported = [
            party
            for party in explicit
            if party["party_type"] not in {"seller", "logistics_provider"}
        ]
        explicit = supported
    if explicit:
        enriched: list[dict[str, Any]] = []
        for party in explicit:
            if party["party_type"] == "seller" and party["party_id"] is None and seller_ids:
                enriched.extend({"party_type": "seller", "party_id": value} for value in seller_ids)
            else:
                enriched.append(party)
        return _unique_parties(enriched)
    if primary_issue == "late_delivery_seller":
        return [{"party_type": "seller", "party_id": value} for value in seller_ids] or [
            {"party_type": "unknown", "party_id": None}
        ]
    if primary_issue == "late_delivery_logistics":
        return [
            {"party_type": "logistics_provider", "party_id": value} for value in logistics_ids
        ] or [{"party_type": "unknown", "party_id": None}]
    if (
        primary_issue
        in {
            "payment_mismatch",
            "duplicate_charge",
            "refund_pending",
            "refund_failed",
            "valid_split_payment",
        }
        and payment_ids
    ):
        return [{"party_type": "payment_provider", "party_id": value} for value in payment_ids]
    if primary_issue != "insufficient_evidence":
        return [{"party_type": "unknown", "party_id": None}]
    return []


def _unique_parties(parties: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for party in parties:
        normalized = {
            "party_type": party["party_type"],
            "party_id": party["party_id"][:128] if isinstance(party["party_id"], str) else None,
        }
        if normalized not in result:
            result.append(normalized)
    return result[:5]


def _root_cause_for(primary_issue: str, responsible: list[dict[str, Any]]) -> dict[str, Any]:
    cause_by_issue = {
        "canceled_order_paid": "CANCELED_ORDER_PAID",
        "unavailable_order_paid": "UNAVAILABLE_ORDER_PAID",
        "late_delivery_seller": "SELLER_DELAY",
        "late_delivery_logistics": "LOGISTICS_DELAY",
        "valid_split_payment": "VALID_SPLIT_PAYMENT",
        "payment_mismatch": "PAYMENT_CAPTURE_MISMATCH",
        "duplicate_charge": "DUPLICATE_CAPTURE",
        "refund_pending": "REFUND_PENDING",
        "refund_failed": "REFUND_FAILED",
        "unsupported_claim": "UNSUPPORTED_CLAIM",
    }
    cause = cause_by_issue.get(primary_issue)
    if cause is None:
        return {"ranked_causes": [], "responsible_parties": []}
    return {
        "ranked_causes": [{"cause_code": cause, "rank": 1}],
        "responsible_parties": _unique_parties(responsible),
    }


def _secondary_issues_from_verdicts(
    shipment_verdict: str, payment_verdict: str, primary: str
) -> list[str]:
    return _secondary_issues(shipment_verdict, payment_verdict, primary)


def _claim_assessments(
    context: CaseContext,
    all_refs: list[str],
    *,
    shipment_verdict: str,
    payment_verdict: str,
    financial: dict[str, Any],
    conflicts: bool,
    primary_issue: str | None = None,
) -> list[dict[str, Any]]:
    request = context.case.get("customer_request", {})
    claims = request.get("claims", []) if isinstance(request, dict) else []
    if not isinstance(claims, list):
        return []
    shipment_refs = _finding_refs(context, "shipment-agent")
    payment_refs = _finding_refs(context, "payment-refund-agent")
    policy_refs = _finding_refs(context, "policy-agent")
    result: list[dict[str, Any]] = []
    calibrator = ConfidenceCalibrator()
    for claim in claims[:5]:
        if not isinstance(claim, dict) or not isinstance(claim.get("claim_id"), str):
            continue
        topic = str(claim.get("topic", "")).lower()
        verdict = "insufficient_evidence"
        refs: list[str] = []
        if topic == "late_delivery_logistics":
            refs = shipment_refs
            if shipment_verdict == "logistics_delay":
                verdict = "supported"
            elif shipment_verdict == "seller_delay":
                verdict = "partially_supported"
            elif shipment_verdict == "on_time":
                verdict = "unsupported"
        elif topic == "late_delivery_seller":
            refs = shipment_refs
            if shipment_verdict == "seller_delay":
                verdict = "supported"
            elif shipment_verdict == "logistics_delay" or shipment_verdict == "on_time":
                verdict = "unsupported"
        elif topic == "payment_mismatch":
            refs = payment_refs
            if payment_verdict == "capture_mismatch":
                verdict = "supported"
            elif payment_verdict == "reconciled":
                verdict = "unsupported"
        elif topic == "duplicate_charge":
            refs = payment_refs
            if payment_verdict == "duplicate_capture":
                verdict = "supported"
            elif payment_verdict == "reconciled":
                verdict = "unsupported"
        elif topic == "refund_pending":
            refs = payment_refs
            verdict = "supported" if payment_verdict == "refund_pending" else verdict
        elif topic == "refund_failed":
            refs = payment_refs
            verdict = "supported" if payment_verdict == "refund_failed" else verdict
        elif topic == "requested_full_refund":
            refs = _bounded_unique([*payment_refs, *policy_refs])
            eligible = _bool_evidence_value(
                _finding_evidence(context, "policy-agent"),
                ("refund_eligible", "full_refund_eligible"),
            )
            if financial.get("recommended_refund_brl", 0) > 0:
                verdict = "supported"
            elif eligible is False:
                verdict = "unsupported"
        elif topic == "valid_split_payment":
            refs = payment_refs
            if (
                _scalar_value(
                    _finding_evidence(context, "payment-refund-agent"), ("split_payment_valid",)
                )
                is True
                or payment_verdict == "valid_split_payment"
            ):
                verdict = "supported"
            elif payment_verdict == "reconciled":
                verdict = "partially_supported"
        elif topic in {"canceled_order_paid", "unavailable_order_paid"}:
            refs = _bounded_unique([*_finding_refs(context, "entity-order-agent"), *payment_refs])
            if primary_issue == topic:
                verdict = "supported"
            else:
                statuses = _lower_values(
                    _data_records(_finding_evidence(context, "entity-order-agent")),
                    ("order_status", "status"),
                )
                status_conflicts = bool(statuses) and not any(
                    ("cancel" in value)
                    if topic == "canceled_order_paid"
                    else ("unavailable" in value)
                    for value in statuses
                )
                if status_conflicts and payment_verdict != "insufficient_evidence":
                    verdict = "unsupported"
        elif topic == "unsupported_claim":
            refs = _bounded_unique(
                [*shipment_refs, *payment_refs, *_finding_refs(context, "entity-order-agent")]
            )
            if primary_issue == "unsupported_claim":
                verdict = "unsupported"
        if topic == "unsupported_claim":
            # A generic unsupported-claim label has no direct evidence target.
            # Keep the verdict indeterminate unless a separately identified
            # claim provides the contradicted topic.
            verdict = "insufficient_evidence"
            refs = []
        if not refs and verdict != "insufficient_evidence":
            verdict = "insufficient_evidence"
        refs = _bounded_unique(refs)
        result.append(
            {
                "claim_id": claim["claim_id"][:64],
                "verdict": verdict if verdict in CLAIM_VERDICTS else "insufficient_evidence",
                "confidence": calibrator.claim(verdict, refs, conflicts),
                "evidence_refs": refs,
            }
        )
    return result


def _policy_warnings(financial: dict[str, Any], conflicts: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    if conflicts:
        warnings.append("unresolved_source_conflict")
    if financial.get("recommended_refund_brl", 0) == 0 and not financial.get("refund_lines"):
        warnings.append("refund_not_supported")
    return warnings


def _collect_conflicts(context: CaseContext) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for source_name, finding in context.findings.items():
        for evidence in finding.get("evidence", []):
            data = evidence.get("data") if isinstance(evidence, dict) else None
            for record in _walk(data):
                raw_conflicts = _record_value(record, "conflicts")
                if not isinstance(raw_conflicts, list):
                    continue
                for raw in raw_conflicts:
                    if isinstance(raw, dict):
                        normalized = _normalize_conflict(raw, source_name)
                        if normalized is not None and normalized not in conflicts:
                            conflicts.append(normalized)
    return conflicts[:5]


def _verify_output(
    context: CaseContext, output: dict[str, Any], *, contracts: Any = None
) -> list[str]:
    issues: list[str] = []
    if not isinstance(output, dict):
        return ["schema_invalid", "output_not_object"]
    if contracts is not None:
        try:
            contracts.validate_output(output, f"workflow/{context.case_id}")
        except Exception:
            return ["schema_invalid"]
    if output.get("case_id") != context.case_id:
        issues.append("case_id_mismatch")
    if output.get("schema_version") != "day09-l3b-output-v2":
        issues.append("schema_version_mismatch")
    evidence_refs = set(output.get("evidence_refs", []))
    known_refs = set(context.evidence)
    if not evidence_refs <= known_refs:
        issues.append("unknown_evidence_ref")
    resolution = output.get("entity_resolution", {})
    resolved = set(resolution.get("resolved_order_ids", []))
    rejected = set(resolution.get("rejected_candidates", []))
    affected = set(output.get("affected_entities", {}).get("order_ids", []))
    if not resolved <= affected:
        issues.append("resolved_order_not_affected")
    if resolved & rejected:
        issues.append("resolved_rejected_overlap")
    entity_finding = context.findings.get("entity-order-agent", {})
    entity_order_ids = set(
        _ids_from_evidence_list(entity_finding.get("evidence", []), ORDER_ID_KEYS)
    )
    entity_item_ids = set(_ids_from_evidence_list(entity_finding.get("evidence", []), ITEM_ID_KEYS))
    entity_seller_ids = set(
        _ids_from_evidence_list(entity_finding.get("evidence", []), SELLER_ID_KEYS)
    )
    if resolved and not resolved <= entity_order_ids:
        issues.append("resolved_order_not_in_entity_evidence")
    if not set(output.get("affected_entities", {}).get("item_ids", [])) <= entity_item_ids:
        issues.append("item_not_in_entity_evidence")
    if not set(output.get("affected_entities", {}).get("seller_ids", [])) <= entity_seller_ids:
        issues.append("seller_not_in_entity_evidence")
    payment_ids = set(
        _ids_from_evidence_list(_finding_evidence(context, "payment-refund-agent"), PAYMENT_ID_KEYS)
    )
    shipment_ids = set(
        _ids_from_evidence_list(_finding_evidence(context, "shipment-agent"), SHIPMENT_ID_KEYS)
    )
    if not set(output.get("affected_entities", {}).get("payment_references", [])) <= payment_ids:
        issues.append("payment_reference_not_supported")
    if not set(output.get("affected_entities", {}).get("shipment_ids", [])) <= shipment_ids:
        issues.append("shipment_id_not_supported")
    for source_name in ("shipment-agent", "payment-refund-agent"):
        source_ids = set(
            _ids_from_evidence_list(_finding_evidence(context, source_name), ORDER_ID_KEYS)
        )
        if source_ids and not source_ids <= resolved:
            issues.append(f"{source_name}_outside_order_scope")
    customer_id = output.get("customer_context", {}).get("customer_unique_id")
    if customer_id is not None:
        customer_ids = set(
            _ids_from_evidence_list(_finding_evidence(context, "customer-agent"), CUSTOMER_ID_KEYS)
        )
        if customer_id not in customer_ids:
            issues.append("customer_not_supported")
        order_customer_ids = set(
            _ids_from_evidence_list(entity_finding.get("evidence", []), CUSTOMER_ID_KEYS)
        )
        if order_customer_ids and customer_id not in order_customer_ids:
            issues.append("customer_order_context_mismatch")
        hint = context.case.get("customer_unique_id_hint")
        if not order_customer_ids and isinstance(hint, str) and customer_id != hint:
            issues.append("customer_hint_context_mismatch")
    shipment = output.get("shipment_analysis", {})
    sellers = set(output.get("affected_entities", {}).get("seller_ids", []))
    if not set(shipment.get("late_seller_ids", [])) <= sellers:
        issues.append("late_seller_not_supported")
    payment = output.get("payment_analysis", {})
    payment_verdict = payment.get("verdict")
    payment_status_values = {
        str(value).lower()
        for value in _all_values(
            _finding_evidence(context, "payment-refund-agent"),
            ("refund_status", "payment_status", "status"),
        )
    }
    if (
        payment_verdict in {"refund_pending", "refund_failed"}
        and "refunded" in payment_status_values
    ):
        issues.append("pending_or_failed_refund_marked_refunded")
    if payment_verdict == "refunded" and "refund_pending" in payment_status_values:
        issues.append("refunded_marked_pending")
    captured = _decimal_from_public(payment.get("captured_total_brl"))
    refunded = _decimal_from_public(payment.get("refunded_total_brl"))
    refundable = _decimal_from_public(payment.get("refundable_total_brl"))
    for key, value in (
        ("captured_total_brl", captured),
        ("refunded_total_brl", refunded),
        ("refundable_total_brl", refundable),
    ):
        if payment.get(key) is not None and value is None:
            issues.append(f"invalid_{key}")
    if (
        captured is not None
        and refunded is not None
        and refunded > captured
        and not _policy_allows_over_capture(context)
    ):
        issues.append("refunded_exceeds_captured")
    financial = output.get("financial_resolution", {})
    recommended = _decimal_from_public(financial.get("recommended_refund_brl")) or Decimal("0")
    line_total = sum(
        (_decimal_from_public(line.get("amount_brl")) or Decimal("0"))
        for line in financial.get("refund_lines", [])
    )
    if recommended != line_total:
        issues.append("refund_lines_total_mismatch")
    cap = refundable
    if cap is None and captured is not None and refunded is not None:
        cap = max(Decimal("0"), captured - refunded)
    if cap is not None and recommended > cap and not _policy_allows_over_capture(context):
        issues.append("recommended_refund_exceeds_valid_amount")
    if recommended > 0 and not _finding_evidence(context, "payment-refund-agent"):
        issues.append("refund_without_payment_evidence")
    if any(
        isinstance(action, str)
        and any(token in action.casefold() for token in ("refund", "reimburse"))
        for action in output.get("resolution_actions", [])
    ) and (recommended <= 0 or not financial.get("refund_lines")):
        issues.append("refund_action_without_supported_refund")
    primary = output.get("assessment", {}).get("primary_issue")
    root = output.get("root_cause_analysis", {})
    cause_codes = {item.get("cause_code") for item in root.get("ranked_causes", [])}
    expected_cause = {
        "canceled_order_paid": "CANCELED_ORDER_PAID",
        "unavailable_order_paid": "UNAVAILABLE_ORDER_PAID",
        "late_delivery_seller": "SELLER_DELAY",
        "late_delivery_logistics": "LOGISTICS_DELAY",
        "valid_split_payment": "VALID_SPLIT_PAYMENT",
        "payment_mismatch": "PAYMENT_CAPTURE_MISMATCH",
        "duplicate_charge": "DUPLICATE_CAPTURE",
        "refund_pending": "REFUND_PENDING",
        "refund_failed": "REFUND_FAILED",
        "unsupported_claim": "UNSUPPORTED_CLAIM",
    }.get(primary)
    if expected_cause is not None and expected_cause not in cause_codes:
        issues.append("root_cause_mismatch")
    party_types = {item.get("party_type") for item in root.get("responsible_parties", [])}
    if not party_types <= RESPONSIBLE_PARTY_TYPES:
        issues.append("invalid_responsible_party")
    if primary == "late_delivery_seller" and "logistics_provider" in party_types:
        issues.append("seller_logistics_responsibility_conflict")
    if primary == "late_delivery_seller" and party_types - {"seller", "unknown"}:
        issues.append("seller_root_cause_party_mismatch")
    if primary == "late_delivery_logistics" and "seller" in party_types:
        issues.append("logistics_seller_responsibility_conflict")
    if primary == "late_delivery_logistics" and party_types - {"logistics_provider", "unknown"}:
        issues.append("logistics_root_cause_party_mismatch")
    if primary in {
        "payment_mismatch",
        "duplicate_charge",
        "refund_pending",
        "refund_failed",
    } and party_types & {"seller", "logistics_provider"}:
        issues.append("payment_responsibility_conflict")
    normalized_shipment = _finding_analysis(
        context,
        "shipment-agent",
        _normalize_shipment_evidence(_finding_evidence(context, "shipment-agent")),
    )
    normalized_payment = _finding_analysis(
        context,
        "payment-refund-agent",
        _normalize_payment_evidence(_finding_evidence(context, "payment-refund-agent")),
    )
    if primary == "late_delivery_seller" and normalized_shipment.get("verdict") != "seller_delay":
        issues.append("seller_delay_not_supported")
    if (
        primary == "late_delivery_logistics"
        and normalized_shipment.get("verdict") != "logistics_delay"
    ):
        issues.append("logistics_delay_not_supported")
    expected_payment_verdict = {
        "payment_mismatch": "capture_mismatch",
        "duplicate_charge": "duplicate_capture",
        "refund_pending": "refund_pending",
        "refund_failed": "refund_failed",
    }.get(primary)
    if expected_payment_verdict and normalized_payment.get("verdict") != expected_payment_verdict:
        issues.append("payment_issue_not_supported")
    if primary in {"canceled_order_paid", "unavailable_order_paid"}:
        order_statuses = _lower_values(
            _data_records(_finding_evidence(context, "entity-order-agent")),
            ("order_status", "status"),
        )
        expected_status = "cancel" if primary == "canceled_order_paid" else "unavailable"
        if not any(expected_status in value for value in order_statuses):
            issues.append("paid_order_status_not_supported")
        if (
            _as_decimal(normalized_payment.get("captured_total_brl")) is None
            or _as_decimal(normalized_payment.get("captured_total_brl")) <= 0
        ):
            issues.append("paid_order_capture_not_supported")
    if primary == "valid_split_payment":
        payment_records = [
            record
            for record in _data_records(_finding_evidence(context, "payment-refund-agent"))
            if _record_value(record, "payment_value") is not None
        ]
        if len(payment_records) < 2 or normalized_payment.get("verdict") != "valid_split_payment":
            issues.append("split_payment_not_supported")
    if primary == "late_delivery_seller" and shipment.get("timeline_complete") is not True:
        issues.append("seller_delay_incomplete_timeline")
    if primary == "late_delivery_logistics" and shipment.get("timeline_complete") is not True:
        issues.append("logistics_delay_incomplete_timeline")
    actions = output.get("resolution_actions", [])
    if len(actions) != len(set(actions)):
        issues.append("duplicate_action")
    status = output.get("assessment", {}).get("case_status")
    if status == "no_action" and actions:
        issues.append("no_action_has_actions")
    if not 0.0 <= output.get("assessment", {}).get("confidence", -1) <= 1.0:
        issues.append("confidence_out_of_bounds")
    request = context.case.get("customer_request", {})
    raw_claims = request.get("claims", []) if isinstance(request, dict) else []
    topics_by_id = {
        claim.get("claim_id"): str(claim.get("topic", "")).lower()
        for claim in raw_claims
        if isinstance(claim, dict) and isinstance(claim.get("claim_id"), str)
    }
    for claim in output.get("claim_assessments", []):
        if not set(claim.get("evidence_refs", [])) <= evidence_refs:
            issues.append("claim_unknown_evidence_ref")
        if not set(claim.get("evidence_refs", [])) <= known_refs:
            issues.append("claim_ref_not_registered")
        topic = topics_by_id.get(claim.get("claim_id"), "")
        if topic in {"late_delivery_seller", "late_delivery_logistics"}:
            relevant_refs = set(_finding_refs(context, "shipment-agent"))
        elif topic in {
            "payment_mismatch",
            "duplicate_charge",
            "refund_pending",
            "refund_failed",
            "valid_split_payment",
        }:
            relevant_refs = set(_finding_refs(context, "payment-refund-agent"))
        elif topic in {"canceled_order_paid", "unavailable_order_paid"}:
            relevant_refs = set(_finding_refs(context, "entity-order-agent")) | set(
                _finding_refs(context, "payment-refund-agent")
            )
        elif topic == "requested_full_refund":
            relevant_refs = set(_finding_refs(context, "payment-refund-agent")) | set(
                _finding_refs(context, "policy-agent")
            )
        else:
            relevant_refs = set()
        if not set(claim.get("evidence_refs", [])) <= relevant_refs:
            issues.append("claim_irrelevant_evidence_ref")
    return list(dict.fromkeys(issues))


def _decimal_from_public(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return decimal if decimal.is_finite() and decimal >= 0 else None


def _policy_allows_over_capture(context: CaseContext) -> bool:
    value = _bool_evidence_value(
        _finding_evidence(context, "policy-agent"),
        ("allow_refund_over_capture", "refund_over_capture_allowed"),
    )
    return value is True


def _degrade_output(
    output: dict[str, Any], context: CaseContext, issues: list[str]
) -> dict[str, Any]:
    if not isinstance(output, dict):
        output = _build_public_output(context)
    degraded = json.loads(json.dumps(output))
    assessment = degraded.get("assessment")
    current_confidence = assessment.get("confidence", 0.0) if isinstance(assessment, dict) else 0.0
    degraded["assessment"] = {
        "primary_issue": "insufficient_evidence",
        "secondary_issues": [],
        "case_status": "needs_investigation",
        "confidence": min(_clamp_confidence(current_confidence), 0.25),
    }
    degraded["root_cause_analysis"] = {"ranked_causes": [], "responsible_parties": []}
    degraded["financial_resolution"] = {
        "currency": "BRL",
        "recommended_refund_brl": 0.0,
        "refund_lines": [],
    }
    degraded["resolution_actions"] = ["collect_additional_evidence"]
    if "claim_assessments" in degraded:
        degraded["claim_assessments"] = [
            {
                "claim_id": claim["claim_id"],
                "verdict": "insufficient_evidence",
                "confidence": 0.0,
                "evidence_refs": [],
            }
            for claim in degraded["claim_assessments"]
        ]
    return degraded
