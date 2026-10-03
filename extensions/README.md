# K4-Track02-Day17 — Extensions (không chấm điểm)

Hai bài này là thực hành thêm về pipeline dữ liệu cho AI, độc lập với các tiêu chí
bắt buộc và bonus trong [RUBRIC.md](../docs/RUBRIC.md). Làm khi đã hoàn thành phần bắt buộc.

```bash
make flywheel     # python -m extensions.flywheel
make kg           # python -m extensions.kg_demo
```

Trên Windows PowerShell, thay `make flywheel` bằng
`.\.venv\Scripts\python.exe -m extensions.flywheel` và `make kg` bằng
`.\.venv\Scripts\python.exe -m extensions.kg_demo`.

## 1. Flywheel dữ liệu của agent — `traces.py`, `dataset.py`, `features.py`

Trace của agent (cây span OpenTelemetry `gen_ai.*`, xuất từ Ngày 13) →
**làm phẳng đệ quy vào Bronze** (1 hàng/span) → **eval golden set** (phần holdout
`split='eval'`) + **cặp ưu tiên DPO** `(prompt, chosen, rejected)` từ lượt ok-vs-lỗi
→ **decontamination**: bỏ mọi cặp có prompt nằm trong eval set (3 cặp thô → 1 cặp sạch).
`features.py` so sánh `ASOF JOIN` (point-in-time) với join "giá trị mới nhất" bị rò rỉ tương lai.
Dataset ra `datasets/`, dùng cho **Ngày 22** (SFT/DPO).

Bài mở rộng: decontamination mờ (13-gram hoặc tương đồng embedding) để bắt cả
prompt eval bị viết lại.

## 2. Knowledge graph vs vector retrieval — `kg.py`

Docs → bộ ba `(entity, relation, entity)` → graph → **traversal 2-hop thật**
(widget → accessory → Hanoi), đối chiếu với retrieval theo chunk: không chunk nào
chứa cả hai đầu của câu trả lời. Extractor là luật tất định (zero-key); thay bằng
LLM + entity resolution là bài mở rộng.

## Câu hỏi suy ngẫm

1. Bước nào trong `traces → Bronze → datasets` sẽ hỏng *âm thầm* nhất ở production,
   và bạn phát hiện nó bằng cách nào?
2. Bỏ qua decontamination thì metric của bạn "nói dối" ra sao?
3. Một câu hỏi graph trả lời tốt mà chunk retrieval chịu thua — và một câu graph là thừa.
