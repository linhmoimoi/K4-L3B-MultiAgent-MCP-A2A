# L3B Architecture Record

Tài liệu này mô tả kiến trúc có thể kiểm chứng của Pha 2, Pha 3 và Pha 4. Public contract trong
`contracts/schemas/` là nguồn chân lý tối cao; handoff và state nội bộ không được
ghi vào output public. Trace chỉ chứa sự kiện quan sát được, không chứa prompt,
chain-of-thought, API key hoặc dữ liệu bí mật.

## 1. System overview

Mỗi lần gọi `solve_case` tạo một `CaseContext` mới, được định danh bởi đúng một
`case_id`. Coordinator tạo các envelope A2A và phân công theo đồ thị sau:

```text
Input Case
    |
    v
Coordinator / Router
    |
    v
Entity / Order Agent
    |
    +--------------------+--------------------+--------------------+
    |                    |                    |
    v                    v                    v
Customer Agent      Payment / Refund Agent  Shipment Agent
    |                    |                    |
    +--------------------+--------------------+
                         |
                         v
                 Customer / Product context
                         |
                         v
                 Policy Agent
                         |
                         v
                 Conflict Resolver
                         |
                         v
                    Verifier Agent
                         |
                         v
                   Validated Output
```

Trong implementation, Entity/Order Agent thực hiện entity resolution rồi thu
thập order/item/seller/product evidence. Customer Agent chỉ lấy customer history
khi scope cho phép. Bốn nhánh specialist được chạy song song
sau khi entity resolution hoàn tất; Policy Agent vẫn có thể chạy khi order chưa
resolve vì policy version là dữ liệu độc lập của case. Conflict Resolver và
Verifier chỉ nhận state nội bộ đã được giới hạn theo case.

`TraceWriter` ghi `case_received` khi `solve_case` bắt đầu và CLI ghi
`case_finalized` sau khi output đã được validate và ghi file. Các event trung gian
là `task_assigned`, `handoff`, `tool_result_consumed`, `policy_decided` và
`verification_completed`.

## 2. Agent ownership

| Agent | Input nhận vào | Nhiệm vụ | Tool được phép gọi | Output/handoff | Lỗi và chuyển tiếp |
| --- | --- | --- | --- | --- | --- |
| Entity/Order Agent | `case_id`, claimed/candidate order IDs, product scope | Xếp hạng candidate; xác nhận order từ `get_order`; thu thập item, seller và product context sau khi order được resolve | `get_order`, `get_order_items`, `get_sellers`, `get_product_context` | `entity_resolution`, normalized IDs/data, order evidence refs, customer ID candidates | Không có evidence: `not_found`; nhiều order hợp lệ: `ambiguous`; không tự chọn candidate |
| Customer Agent | `case_id`, customer ID từ order evidence hoặc case hint, customer-history scope | Chỉ lấy customer history khi `include_customer_history is True`; chỉ xác nhận customer ID từ MCP response | `get_customer_history` | `customer_context`, related order IDs, customer evidence refs | Scope false hoặc không có ID: không gọi tool; MCP không xác nhận ID thì giữ `null` |
| Coordinator | Case gốc và kết quả entity resolution | Kiểm tra discovery; tạo task; điều phối specialist; giới hạn vòng đời và chuyển tiếp | Không được gọi MCP | A2A task envelopes; tổng hợp findings cho Conflict Resolver | Không tự suy luận domain; specialist lỗi được chuyển thành thiếu evidence và đưa sang Conflict Resolver |
| Shipment Agent | Resolved order IDs | Đọc shipment summary và timeline completeness | `get_shipment_summary` | Shipment evidence refs và verdict quan sát được | Thiếu summary: `insufficient_evidence`; chuyển finding cho Coordinator |
| Payment/Refund Agent | Resolved order IDs | Đối soát payment capture, payment timeline và refund timeline | `get_order_payments`, `get_payment_timeline`, `get_refund_timeline` | Payment/refund evidence refs, totals và verdict nếu MCP nêu rõ | Không đoán tổng tiền/refund khi thiếu evidence; chuyển finding rỗng |
| Policy Agent | `policy_version` của case | Lấy chính sách và trả decision/evidence để policy có thể được kiểm tra độc lập | `get_policy` | Policy evidence refs; `policy_decided` trace event | Không có policy: decision code `insufficient_evidence`; vẫn chuyển tiếp |
| Conflict Resolver | Specialist findings và evidence refs cùng case | Chuẩn hóa conflict được MCP báo cáo, giữ source và selected source; không tự tạo conflict | Không được gọi MCP | `data_conflicts` nội bộ cho Verifier | Conflict chưa giải quyết giữ `selected_source: null` và resolution code rõ ràng |
| Verifier Agent | Entity result, specialist findings, conflict result | Kiểm tra invariant, giới hạn public fields, dựng và validate L3B output | Không được gọi MCP | Public object đúng `l3b-output-v2.schema.json`; `verification_completed` | Không đủ evidence thì output hợp lệ nhưng dùng verdict `insufficient_evidence`, null/empty đúng schema |

MCP discovery chỉ cho biết tool tồn tại. Discovery không cấp quyền. `ToolBroker`
kiểm tra đồng thời discovery và ma trận permission trước mọi call; agent không
thể gọi tool ngoài tập quyền của mình.

## 3. Coordinator, entity resolution và handoff

Coordinator dùng protocol một chiều, tránh vòng lặp:

1. Nhận case và phát `case_received`; tạo context mới, không dùng state global.
2. Giao `resolve_entities` cho Entity/Order Agent.
3. Giao bốn task specialist bằng các message có cùng `case_id` và danh sách order
   đã resolve.
4. Giao kết quả cho Conflict Resolver, sau đó giao conflict result cho Verifier.
5. Chỉ Verifier được tạo public output; caller validate và finalize sau đó.

Candidate được thử theo thứ tự claimed order ID rồi candidate list, loại trùng.
Candidate chỉ được coi là resolved khi `get_order` trả evidence hợp lệ; nếu
MCP trả order ID khác với ID đang hỏi, candidate bị reject. Một candidate thành
công cho trạng thái `resolved`, nhiều candidate thành công cho `ambiguous`, và
không có candidate thành công cho `not_found`. Failure kết nối không được biến
thành rejected candidate vì đó không phải bằng chứng loại trừ.

Envelope A2A nội bộ có dạng logic:

```text
{
  case_id: str,
  message_id: str,
  sender: agent_id,
  recipient: agent_id,
  task: str,
  payload: internal-only object,
  evidence_refs: list[str],
  normalized_data: internal-only object,
  warnings: list[str],
  errors: list[str]
}
```

`case_id` là bắt buộc ở mọi envelope và được kiểm tra trước khi workflow chạy.
`message_id`, agent IDs, task names, normalized data, warnings và errors không
được đưa vào public output.
Handoff trace chỉ ghi actor, target, task không nhạy cảm và evidence refs; không
ghi payload suy luận riêng.

## 4. Tool permission matrix

| Tool | Entity/Order | Customer | Coordinator | Shipment | Payment/Refund | Policy | Conflict | Verifier |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `get_customer_history` | — | ✓ | — | — | — | — | — | — |
| `get_order` | ✓ | — | — | — | — | — | — | — |
| `get_order_items` | ✓ | — | — | — | — | — | — | — |
| `get_order_payments` | — | — | — | — | ✓ | — | — | — |
| `get_payment_timeline` | — | — | — | — | ✓ | — | — | — |
| `get_policy` | — | — | — | — | — | ✓ | — | — |
| `get_product_context` | ✓ | — | — | — | — | — | — | — |
| `get_refund_timeline` | — | — | — | — | ✓ | — | — | — |
| `get_sellers` | ✓ | — | — | — | — | — | — | — |
| `get_shipment_summary` | — | — | — | ✓ | — | — | — | — |

Trước call, broker phải thấy tool trong kết quả `list_tools`, tên tool phải thuộc
allowlist và agent phải có quyền trong bảng. Mọi call đều truyền
`case_id=context.case_id`; không có API key trong arguments, trace hay handoff.

`EvidenceGateway` là boundary duy nhất tới MCP session: nó thêm `case_id` vào
payload, đọc `structuredContent` hoặc một text JSON duy nhất, rồi gọi
`Contracts.validate_evidence`. Response lỗi của MCP trở thành gateway error và
không được đăng ký vào evidence cache. Specialist không biết endpoint HTTP và
không truy cập trực tiếp MCP session.

## 5. Entity resolution protocol

Input resolution chỉ dùng candidate IDs có trong case. Broker cache kết quả
`get_order` theo `(tool_name, normalized_arguments)` trong context hiện tại.
Evidence envelope đã được schema validator kiểm tra trước khi được lưu; ref lấy
nguyên từ MCP, không tự sinh hoặc sửa.

Confidence resolution là deterministic: `1.0` cho đúng một candidate evidence-backed,
`0.5` cho nhiều candidate hợp lệ và `0.0` khi không có candidate hợp lệ. Ngưỡng
chuyển tiếp là một order duy nhất; với `ambiguous` hoặc `not_found`, specialist
order-scoped không tự chọn order và chỉ trả thiếu evidence. Customer history được
giao cho Customer Agent riêng và chỉ được truy vấn khi scope bật cùng với customer
ID từ order evidence hoặc customer hint của case.

## 6. Evidence lifecycle

Evidence đi qua các trạng thái:

```text
MCP discovery → bounded call → schema validation → case-local registry/cache
       → tool_result_consumed trace → specialist finding → verifier/public ref
```

Mỗi response phải có `schema_version`, `evidence_ref`, `result_hash`, `domain` và
`data` theo `mcp-evidence-response-v1.schema.json`. `tool_result_consumed` luôn
có đúng `case_id`, `tool_name` và evidence refs. Cache hit không tạo thêm MCP
call; một evidence ref chỉ phát event `tool_result_consumed` một lần trong phạm
vi case để tránh trace dư thừa.

Evidence registry nằm trong `CaseContext`, được tạo lại cho từng case và không có
cache process-global. Vì vậy evidence ref của case này không được truyền sang
case khác. Output chỉ lấy refs đã đăng ký từ response MCP; verifier không tạo
ref giả. Nếu MCP thiếu, timeout hoặc trả envelope lỗi, output dùng empty/null và
verdict `insufficient_evidence`, không dùng dữ liệu fallback.

## 7. Conflict lifecycle

Conflict Resolver chỉ nhận conflict được explicit trong MCP data. Mỗi conflict
được giữ với `field`, ít nhất hai `sources`, `selected_source` (có thể null) và
`resolution_code` trước khi chuyển cho verifier. Resolver không tự chọn nguồn chỉ
vì một agent chạy trước agent khác. Policy precedence chỉ được áp dụng khi MCP
đã trả policy/evidence tương ứng; nếu không đủ thì giữ conflict unresolved.

Verifier map conflict vào `data_conflicts` và hạ kết luận về trạng thái cần điều
tra khi invariant không chứng minh được. Internal conflict object không rò rỉ
ngoài các field public được schema cho phép.

## 8. Trace và correlation

| Event | Actor/target | Nội dung tối thiểu |
| --- | --- | --- |
| `case_received` | Coordinator | `case_id`, actor |
| `task_assigned` | Coordinator → agent | `case_id`, target, task attribute |
| `handoff` | sender → recipient | `case_id`, target, task attribute, evidence refs nếu có |
| `tool_result_consumed` | owning agent | `case_id`, `tool_name`, evidence refs |
| `policy_decided` | Policy Agent | `case_id`, decision code, policy evidence refs nếu có |
| `verification_completed` | Verifier | `case_id`, decision code, output evidence refs |
| `case_finalized` | Coordinator/CLI | `case_id`, actor, sau validate output |

Mọi event được validate bằng `trace-event-v1.schema.json` trước khi append. Event
không có field ngoài schema. `target` có mặt cho handoff; tool consumption có
`tool_name` và `evidence_refs`. Trace không chứa prompt, chain-of-thought, API key,
raw secret, internal message payload hoặc exception stack.

## 9. Retry, timeout và fallback policy

| Failure | Retry budget | Fallback | Trace/decision |
| --- | ---: | --- | --- |
| MCP timeout / transient runtime failure | 1 retry, timeout 15 giây mỗi attempt | Bỏ qua response; không tạo evidence giả | Không emit consumption cho response lỗi; verifier dùng thiếu evidence |
| Tool không có trong discovery | 0 | Không gọi tool; agent trả finding rỗng | `insufficient_evidence` ở policy/output khi phù hợp |
| Entity not found | 1 attempt theo candidate đã cho | `not_found`, không reject nếu chỉ là network failure | Handoff entity result |
| Entity ambiguous | Không gọi thêm ngoài candidate scope | `ambiguous`, specialist không tự chọn | Handoff với các resolved IDs |
| Source conflict | 0 MCP retry ở resolver | Preserve unresolved conflict | `data_conflicts`, policy/verifier decision |
| Invalid specialist result | 0 retry ở agent result | Cô lập agent; verifier dùng evidence còn hợp lệ | `verification_completed` với kết quả degraded |

Mỗi case có tối đa 32 MCP attempts. Retry chỉ áp dụng cho call idempotent và
không làm thay đổi input. Failure không được cache thành evidence; response lỗi
chỉ được cache như miss của đúng key để tránh retry vô hạn trong cùng case.

## 10. Verification invariants

Trước khi return, Verifier kiểm tra hoặc bảo đảm các invariant sau:

- output chỉ có field được phép bởi `l3b-output-v2.schema.json` và đúng schema version;
- `case_id` output, trace và mọi envelope trùng case đang xử lý;
- resolved/rejected candidate chỉ xuất hiện khi có căn cứ resolution tương ứng;
- mọi public `evidence_ref` có trong case-local MCP registry và không vượt giới hạn schema;
- claim assessments không vượt 5, claim ID lấy từ input case, và claim thiếu evidence là `insufficient_evidence`;
- shipment verdict, payment verdict, totals, refund lines và action không được suy ra từ missing data;
- tổng tiền không âm, currency là `BRL`, confidence nằm trong `[0, 1]`;
- root cause và responsible party chỉ được nêu khi có finding/evidence tương ứng;
- conflict public giữ source/selected source hợp lệ và không nhúng internal payload;
- output được `Contracts.validate_output` trước khi caller ghi file.

Nếu invariant domain không thể chứng minh vì MCP thiếu evidence, output vẫn phải
đúng public schema nhưng dùng trạng thái thiếu evidence và tập field rỗng/null
phù hợp. Đây là trạng thái có chủ ý, không phải dữ liệu fallback.

## 11. Query budget và cache strategy

Cache key là tool name cộng normalized, sorted arguments. Cache nằm trong một
`CaseContext`; cache hit không làm phát sinh MCP call mới. Entity Agent thực hiện
candidate resolution trước, specialist chỉ nhận order IDs đã resolve duy nhất.
Các specialist độc lập nên có thể chạy song song; broker vẫn áp dụng budget dùng
chung của case.

Budget 32 attempts/case, timeout 15 giây/call, retry tối đa một lần. Không scan
ngoài candidate IDs, không gọi tool ngoài scope của case và không gọi lại cùng
key khi đã có response hoặc miss đã được ghi nhận. Discovery được thực hiện một
lần trước specialist calls.

## 12. Public contracts và submission manifest

Workflow không sửa hoặc mở rộng các schema công khai. `l3b-output-v2.schema.json`
là contract của output; `trace-event-v1.schema.json` là contract của trace;
`mcp-evidence-response-v1.schema.json` được dùng để validate từng response MCP;
`submission-manifest-v2.schema.json` chỉ được dùng ở bước package/validate artifact
ở pha sau. Internal A2A fields không xuất hiện trong bất kỳ contract public nào.

## 13. Reproducibility

Workflow hiện là deterministic về routing, permission, candidate order, cache key,
retry count, timeout, budget và mapping output. Không dùng model tự sinh hoặc
random seed cho kết luận nghiệp vụ; `message_id` chỉ là định danh nội bộ không
được dùng làm dữ liệu nghiệp vụ. Dependency ranges được pin theo `pyproject.toml`;
lệnh kiểm tra Pha 2/Pha 3/Pha 4 gồm syntax/import, unit tests gateway-specialist-trace,
schema validation và lint. `.env`, API key, case payload riêng và chain-of-thought không được ghi vào
architecture, trace hay package.

## 14. Policy engine and business decisions (Phase 4)

The Policy Agent's decision stage runs only after the case-local specialist
findings are available. Its policy fetch may run in parallel with independent
specialists, but the decision input is the original case claims, entity resolution, order and
item findings, shipment findings, payment/refund findings, customer/product
context, policy evidence, and normalized conflict records. It does not call a
scoring policy as if it were business policy: `contracts/scoring/
scoring-policy-v2.json` describes evaluation weights only. The business rules
for a case come from the MCP `get_policy` response for that case's
`policy_version`.

The decision procedure is deterministic and evidence-first:

1. An explicit, schema-valid issue from policy evidence has precedence.
2. Otherwise shipment and payment specialist verdicts are mapped to the
   allowed public `primary_issue` enum.
3. A missing, ambiguous, or conflicting basis produces
   `insufficient_evidence` or `needs_investigation`; it is never converted to
   a guessed business outcome.
4. Responsible parties are taken from policy evidence when valid, otherwise
   from the entity and specialist evidence appropriate to the selected root
   cause. An unsupported assignment becomes `unknown` with a null ID.
5. Resolution actions are deduplicated and are selected from policy evidence
   or the bounded issue-to-action mapping. Investigation cases receive only
   evidence-collection action.

### Evidence normalization used by Phase 4

The MCP payload is not assumed to be a flat object. Normalization walks only
case-local `data` dictionaries/lists after the gateway has validated the
evidence envelope. In particular, the implementation reads:

- order status and identity from `get_order` data;
- item/seller/product IDs from either an item list or nested item rows;
- shipment dates from `delivered_carrier_at`, `delivered_customer_at`,
  `estimated_delivery_at`, and seller deadlines in `shipping_limits`;
- payment values from the order-payment list or timeline `payments`, while
  deduplicating the same server record returned by both tools;
- payment/refund state only from timeline `event_type`/`status` markers and
  their amounts; a repeated payment sequence by itself is not a duplicate
  capture; and
- business decisions from `get_policy.data.rules[primary_issue]`, never from
  a different rule in the same policy response.

Shipment is `seller_delay` when carrier handoff is later than an evidenced
seller deadline. It is `logistics_delay` only when seller handoff is on time
and customer delivery is later than the evidenced estimate. `on_time` needs
the complete timeline. Payment mismatch and duplicate charge require a
timeline mismatch/duplicate marker or an evidenced capture amount conflict;
multiple distinct payment methods are treated as a valid split only if no
stronger payment fault is present. Canceled/unavailable paid orders require
both the order status and a positive evidenced capture.

The responsibility guardrails are:

| Root cause | Allowed responsibility basis |
| --- | --- |
| `SELLER_DELAY` | seller ID from resolved order/item evidence |
| `LOGISTICS_DELAY` | carrier/logistics ID from shipment evidence |
| payment capture mismatch, duplicate capture, or refund failure/pending | payment provider ID from payment evidence |
| unsupported or unresolved issue | `unknown` or no responsible party |

The financial resolver uses `Decimal` and quantizes to BRL cents until the
final public conversion. A recommendation uses only the selected business
policy rule's `refund_brl` plus payment evidence for the resolved order. It
is capped at `refundable_total_brl` (or captured minus refunded); without a
known payment cap it is zero. Generated refund lines use the selected issue
and resolved order ID, and their sum must equal the recommendation. An
inconsistent or over-cap recommendation becomes zero refund lines rather than
being invented or silently rounded into validity.

Claim assessments are produced only for claim IDs present in the input. Each
claim receives a verdict from the evidence domain that can support that claim,
and its refs are restricted to those consumed by that domain. Refs from an
unrelated specialist are not used as claim support.

## 15. Verifier invariants and degradation

The Verifier Agent is the only component that returns the public output from
the orchestration graph. Before returning it checks the public contract and
cross-field invariants, including:

- schema version, case correlation, required fields, enum values, unique ID
  sets, additional fields, and evidence-ref ownership;
- resolved orders versus affected orders and rejected candidates;
- customer identity, seller/item/order scope, and shipment ownership;
- seller versus logistics delay, timeline completeness, and compatible
  responsibility party;
- non-negative payment totals, captured/refunded/refundable relationships,
  pending-versus-refunded status, duplicate-charge support, and payment
  mismatch support;
- primary issue versus ranked root cause and responsible parties;
- unique actions, `no_action` consistency, and investigation status;
- recommended refund versus refund-line totals and the valid refund cap;
- claim evidence refs being a subset of the case evidence registry.

The verifier does not repair a bad value by guessing. If an invariant can be
recomputed from evidence, the output is rebuilt from the same normalized
findings. If it cannot be proven, the public decision is degraded to
`insufficient_evidence`, confidence is capped at `0.25`, financial resolution
is zero, claims become `insufficient_evidence`, and the action is
`collect_additional_evidence`. The original invariant codes remain internal in
the verifier finding and are not leaked into the public contract.

## 16. Confidence calibration

`ConfidenceCalibrator` is deterministic and bounded to `[0.0, 1.0]`. The
overall score is the weighted sum of these observable factors:

| Factor | Weight |
| --- | ---: |
| resolved entity quality | 0.18 |
| required evidence presence | 0.18 |
| direct support for the selected issue | 0.22 |
| absence of source conflict | 0.14 |
| shipment/payment timeline completeness | 0.10 |
| policy evidence present | 0.10 |
| specialist consistency | 0.08 |

The score is clamped and rounded after calculation, with a production maximum
of `0.95` even for complete, consistent evidence. `insufficient_evidence` is
capped at `0.25`, ambiguous or unresolved entity resolution at `0.45`, and
any unresolved conflict at `0.55`. Missing refs give a claim confidence of
zero. Supported, unsupported, and partially supported claims use fixed
deterministic bases and are reduced when conflict exists. Confidence never
overrides contract errors and is not used to justify data absent from MCP.

## 17. Phase 4 lifecycle and decision codes

For one case the observable sequence is:

```text
case_received
  -> task_assigned
  -> tool_result_consumed
  -> handoff
  -> policy_decided
  -> verification_completed
  -> case_finalized
```

The CLI owns `case_finalized`; `solve_case` owns all events before it, so the
same lifecycle event is not emitted twice. `policy_decided` is emitted by the
Policy Agent with `policy_decision`, `conflict_detected`, or
`insufficient_evidence` as the decision code. The verifier emits `passed` when
no invariant fails and `degraded` when it must enter the evidence-insufficient
state. Every decision event carries only the case ID and refs already present
in the case-local registry. In particular, `policy_decided` includes the
server-issued `get_policy` ref whenever policy evidence was consumed, even if
the resulting issue is `insufficient_evidence`. Trace events are validated against
`trace-event-v1.schema.json` before append.

## 18. Missing, ambiguous, and conflicting evidence

Timeouts, unavailable tools, malformed envelopes, absent entities, and
authorization failures do not create synthetic evidence. They produce an
empty finding or an explicit internal error, preserve the case-local evidence
refs that do exist, and allow the verifier to choose a valid insufficient
state. An ambiguous entity never becomes a resolved order merely because the
case claimed that ID. A conflict keeps its source list and null selected source
unless MCP policy evidence supplies a valid selection; conflict lowers
confidence and can force investigation status.
