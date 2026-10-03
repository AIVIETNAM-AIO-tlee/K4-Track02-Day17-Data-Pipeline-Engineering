# Vibe Coding — Tips chung để tối ưu workflow

> Đọc 5–10 phút. Áp dụng cho mọi project, không chỉ Lab 17.

## Vibe coding là gì?

**Vibe coding** (Andrej Karpathy, 02/2025) — bạn để LLM viết phần lớn code,
còn bạn đảm nhận vai *architect* và *reviewer*: mô tả intent → review diff
→ accept hoặc reject. Bạn không gõ từng dòng `for` loop; bạn ép spec rõ
ràng và đảm bảo không có bug ngầm trong diff trả về.

Vibe coding ≠ "copy-paste từ ChatGPT". Vibe coding là một *workflow*:

```
   intent (spec)
      ↓
   prompt LLM
      ↓
   review diff (không skip!)
      ↓
   run + verify
      ↓
   commit hoặc rollback
```

Bỏ qua bất kỳ bước nào → vibe coding biến thành "gambling with code".

---

## 2 phong cách kỷ luật: SDD và TDD

### Spec-Driven Development (SDD)

> Viết **spec** trước, code sau. Spec là contract giữa bạn và LLM.

**Spec đầy đủ** thường gồm:
- *Inputs:* tên + kiểu + ràng buộc của mỗi tham số
- *Output:* shape + kiểu + invariants
- *Behavior:* edge case, lỗi, side-effects
- *Constraints:* latency budget, memory cap, dependency cấm dùng

Ví dụ:
```
Function: validate_events(records: list[tuple]) -> tuple[list[dict], list[dict]]
Inputs:
  - records: (_payload JSON text, partition, offset, _ingested_at, _batch_id) from Bronze
Output:
  - (valid rows ready for Silver, bad rows with a human-readable reason)
Behavior:
  - type in {click, feedback}; feedback needs rating up/down; click has no rating
  - user_id and event_id non-empty; event_time parsed and stored as naive UTC
  - one bad record never raises — it goes to the bad list
Constraints:
  - pure function, no I/O, deterministic order
  - a silently dropped event is a SILENT regression — think hard here
```

LLM viết code khớp spec. Bạn review diff để verify từng dòng implement
đúng spec. Spec mơ hồ → code mơ hồ → debug 1 giờ.

### Test-Driven Development (TDD) cho LLM era

> Viết **test** trước, code sau. Test là spec dạng máy chấm.

```
Vòng 1: "Write a pytest test for validate_events that asserts:
         - a feedback event with rating 'meh' is rejected with a reason
         - a click event that carries a rating is rejected
         - event_time '2026-08-12T21:10:00Z' comes back as naive UTC 21:10
         Don't write the implementation yet — only the test."
```

Test pass-by-construction (bạn run, test fail vì chưa có code). Sau đó:

```
Vòng 2: "Now implement the three functions such that the test passes."
```

Vibe code phần implement, nhưng **test là không đổi**. Nếu test sai (ví dụ
assert sai logic), bạn phát hiện ngay từ vòng 1, không phải sau khi
deploy.

TDD đặc biệt mạnh với vibe coding vì LLM hay hallucinate edge cases —
tests làm bộ chống hallucination.

---

## Khi nào vibe code, khi nào tự nghĩ?

| Vibe code thoải mái | Tự nghĩ kỹ trước khi prompt |
|---|---|
| API route boilerplate (FastAPI, Express, …) | Lựa chọn algorithm / data structure cốt lõi |
| Pydantic / Zod / Typescript schemas | Concurrency model (lock vs lock-free vs CAS) |
| Test scaffolding (pytest fixtures, mocks) | Failure semantics (retry, idempotency) |
| Config files (YAML, JSON, env) | Schema migration / backward compat |
| README skeleton, docstrings | Security boundary (auth, sandboxing) |
| Synthetic data generators / fixtures | Performance budget tradeoffs |
| Error handling cho I/O (try/except boilerplate) | Cache invalidation strategy |
| Refactor "đổi tên field X → Y" trên cả repo | Architecture (vector vs graph, monolith vs micro) |

**Quy tắc đơn giản:** nếu bug sẽ là *silent regression* (hệ thống chạy
nhưng kém hơn, không lỗi rõ) thay vì *loud failure* (exception, test
fail), đó là **think-hard zone**. Đừng để LLM tự quyết.

---

## 5 prompt patterns universal

### 1. Specs in, code out

> Càng narrow → cleaner diff, ít iterate hơn.

```
[VAGUE — DON'T]
"Write a search API"

[NARROW — DO]
"FastAPI GET /search?q=str&mode=Literal['kw','sem','hybrid']
returning SearchResponse(query: str, mode: str, latency_ms: float, hits: list[Hit]).
Use Searcher class from app/search.py — call .search(q, mode=mode, top_k=10).
Measure latency_ms with time.perf_counter() server-side, exclude network."
```

### 2. Validate trước khi generate

> Với công thức / thuật toán: hỏi AI giải thích, cross-check, mới nhờ implement.

```
Step 1: "Explain DuckDB QUALIFY row_number() OVER (PARTITION BY k ORDER BY ts DESC) = 1.
         If two rows tie on ts, which one survives? Is it the same on every run?"
Step 2: Bạn cross-check answer với DuckDB docs (QUALIFY, window functions).
Step 3: "Keep the latest export per ticket_id, with a tie-breaker that makes it deterministic."
```

AI hay quên tie-breaker: kết quả đúng lúc test, nhưng thỉnh thoảng chọn hàng khác
khi chạy lại — checksum "nhảy" mà không ai đổi code. Silent regression.

### 3. Tests trước, code sau (TDD)

> Test là spec dạng máy chấm. Viết test trước → code phải pass test.

```
"Write a pytest test that asserts X. Don't write implementation yet."
```

Sau khi test viết đúng (run pass-by-construction = fail), prompt implement.

### 4. Minimal repro → expand

> Đừng yêu cầu LLM viết toàn bộ feature trong 1 prompt. Build incrementally.

```
Step A: "Write minimal X with 1 feature."
Step B: "Run + verify."
Step C: "Now extend X to handle case Y."
Step D: "Now wrap in benchmark/test loop."
```

LLM không hallucinate khi context đã có working baseline.

### 5. Plan → code → review loop

> 3 vòng: AI propose 3 approaches → bạn pick → AI implement → bạn review.

```
Vòng 1: "Propose 3 approaches to do X. Compare on (cost, complexity, scalability)."
Vòng 2: Bạn pick 1: "Use approach #2 because Z."
Vòng 3: "Implement approach #2 + write test."
Vòng 4: Bạn review diff line-by-line.
```

Đừng skip vòng 1 — bạn sẽ stuck trong local optimum mà LLM nghĩ ra đầu tiên.

---

## CLI tool recommendations

Lab này khuyến khích **CLI vibe-coding** — git-native, terminal-friendly,
review diff dễ. Ba CLI tool nổi bật 2026:

| Tool | Best at | Weak at |
|---|---|---|
| **Claude Code** (Anthropic) | Multi-file plans, careful edits, longer reasoning, in-terminal TodoWrite + plan mode | Slower cho 1-line fixes; cần Anthropic API key hoặc subscription |
| **Codex CLI** (OpenAI) | Fast iteration, tight integration với GPT/o1 family, agent mode chạy command thực | Mới hơn, ecosystem still evolving; cần OpenAI key |
| **OpenCode** (open-source) | Terminal-first TUI, multi-provider (Anthropic/OpenAI/local Ollama), no vendor lock-in | Smaller community than Claude Code; cần config provider lần đầu |

**Why CLI over IDE?** Trong terminal bạn dễ:
- Review diff (`git diff`) trước khi accept
- Pipe output qua tools khác (`benchmark.py | grep PASS`)
- Reproduce y hệt trên server / CI / pair programming session
- Commit + push + revert mà không rời context

**Project conventions file** — commit 1 file ở repo root để CLI tool tự
đọc + respect, giảm prompt boilerplate:
- `CLAUDE.md` — Claude Code
- `AGENTS.md` — Codex CLI, OpenCode (de-facto standard 2025+)

Đa số CLI tool đọc fallback tới `AGENTS.md` nếu không có file riêng,
nên 1 file `AGENTS.md` thường đủ cho cả 3 tool. Không cần duplicate.

---

## 3 anti-patterns phổ biến

### 1. Hỏi AI quyết định kiến trúc

❌ "Which embedding model should I use?"
→ AI pick default trong training data, không biết corpus của bạn.

✅ "I have a 1M-doc Vietnamese corpus, GPU=A10, latency budget=20ms. List 3
   candidate embedding models with (MTEB-vi score, dim, RAM/1M vecs, cost).
   Recommend top 1, explain why."

### 2. Generate-and-trust không test

❌ Accept AI-written code → commit → push → discover bug in prod.

✅ AI generate → bạn run test → test pass → review diff → commit.
   Nếu chưa có test, viết test trước (pattern #3 ở trên).

### 3. "Make it faster" không có số

❌ "Make this latency faster"
→ AI optimize ngẫu nhiên, có thể slower.

✅ "P99 hiện tại = 87ms (measured by `<command>`). Target < 50ms.
    Profile shows 60% time in `<function>`. Suggest 3 optimizations with
    expected speedup."

### 4. Bonus — prompt thiếu context

❌ "Fix this bug"

✅ Paste exact error message + expected output + minimal repro + relevant
   file paths + last commit that worked. Mơ hồ in → mơ hồ out.

---

## Workflow điển hình cho 1 task

```
1. Đọc / viết spec (5 phút)         → bạn nghĩ
2. Plan: think-hard zone? (1 phút)  → bạn nghĩ
3. Prompt với spec rõ                → AI sinh
4. Review diff line-by-line          → bạn xác minh
5. Run test / benchmark              → máy verify
6. Commit hoặc rollback              → bạn quyết định
```

Không skip step 4 và 5. Đó là chỗ vibe coding fail-soft thay vì
fail-loud.

---

## Đọc thêm

- Andrej Karpathy — "Vibe coding" tweet (02/2025)
- Simon Willison — "Vibe coding is here, and it's pretty cool" (02/2025)
- Anthropic — "Effective coding with Claude" docs
- Geoffrey Litt — "Malleable software" essay (2024) — context cho tại sao vibe coding work

Tốt — giờ áp dụng những patterns này khi làm Lab 17. Đọc docstring từng module, viết
spec rõ trong đầu, prompt AI, review, chạy `make verify` + `make rerun3`, commit.
