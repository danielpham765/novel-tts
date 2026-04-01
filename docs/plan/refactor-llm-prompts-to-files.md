# Refactor: Extract LLM Prompts to Config Files

## Context

Toàn bộ 10 prompt gửi tới Gemini đang bị hardcode trực tiếp trong source Python. Điều này khiến việc chỉnh sửa prompt yêu cầu sửa code, khó theo dõi diff, và dễ bỏ sót khi so sánh các biến thể. Mục tiêu là chuyển toàn bộ phần text tĩnh của prompt ra file `configs/llm-prompts/`, code chỉ còn inject dữ liệu động (base_rules, glossary, text cần dịch).

---

## Phạm vi thay đổi

### Files tạo mới

**10 prompt template files** trong `configs/llm-prompts/`:

| File | Function sử dụng | Placeholders |
|------|-----------------|--------------|
| `translate-primary.txt` | `_generate_translation_chunk()` | `{base_rules}`, `{glossary_text}`, `{text}` |
| `translate-safe-literary.txt` | `_safe_literary_prompt()` | `{base_rules}`, `{glossary_text}`, `{text}` |
| `translate-softened.txt` | `_generate_translation_chunk()` (fallback cuối) | `{base_rules}`, `{glossary_text}`, `{text}` |
| `translate-cleanup.txt` | `final_cleanup()` | `{base_rules}`, `{glossary_text}`, `{text}` |
| `translate-han-repair-line.txt` | `patch_remaining_han()` | `{base_rules}`, `{glossary_text}`, `{text}` |
| `translate-han-repair-aggressive.txt` | `aggressive_repair_han()` | `{base_rules}`, `{glossary_text}`, `{text}` |
| `translate-repair-source.txt` | `repair_against_source()` | `{base_rules}`, `{source_text}`, `{translated_text}` |
| `translate-repair-placeholders.txt` | `repair_placeholder_tokens_against_source()` | `{base_rules}`, `{examples}`, `{source_text}`, `{translated_text}` |
| `translate-glossary-extract.txt` | `_extract_glossary_updates()` | `{compacted_suffix}`, `{compact_source}`, `{compact_translated}` |
| `caption-translate.txt` | `translate_captions()` | `{glossary_section}`, `{batch_json}` |

**1 loader module**: `novel_tts/translate/prompts.py`

### Files sửa

- `novel_tts/translate/novel.py` — 9 chỗ inline prompt
- `novel_tts/translate/captions.py` — 1 chỗ inline prompt (build `prompt_parts` list)

---

## Thiết kế loader (`novel_tts/translate/prompts.py`)

```python
from pathlib import Path
from functools import lru_cache

def _prompts_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "configs" / "llm-prompts"

@lru_cache(maxsize=None)
def _load_raw(name: str) -> str:
    return (_prompts_dir() / name).read_text(encoding="utf-8")

def render_prompt(name: str, **kwargs: str) -> str:
    """Load template file and replace {placeholder} markers with kwargs values."""
    text = _load_raw(name)
    for key, value in kwargs.items():
        text = text.replace("{" + key + "}", value)
    return text
```

Dùng `str.replace` thay vì `.format()` để tránh lỗi khi `base_rules` hoặc `glossary_text` có chứa ký tự `{` `}`.

---

## Nội dung từng prompt file

### `translate-primary.txt`
```
{base_rules}
Glossary dùng bắt buộc nếu xuất hiện:
{glossary_text}

Hãy tự kiểm tra và sửa ngay trong một lần trả lời trước khi xuất kết quả cuối cùng. Tuyệt đối không để xuất hiện kiểu phiên âm máy/pinyin lẫn vào tiếng Việt như 'tha', 'ngã', 'nhĩ', 'liễu', 'thập ma', 'chẩm hội'.

Dịch đoạn sau sang tiếng Việt:
{text}
```

### `translate-safe-literary.txt`
```
{base_rules}
Glossary dùng bắt buộc nếu xuất hiện:
{glossary_text}

Đây là đoạn văn học hư cấu từ tiểu thuyết mạng. Nếu có cảnh thân mật hoặc nội dung người lớn, hãy chuyển ngữ bằng giọng văn trung tính, tiết chế, không thêm chi tiết nhạy cảm, không tăng mức độ gợi dục, nhưng vẫn giữ nguyên ý và mạch truyện. Chỉ trả về đúng bản dịch tiếng Việt.

Dịch đoạn sau sang tiếng Việt:
{text}
```

### `translate-softened.txt`
```
{base_rules}
Glossary dùng bắt buộc nếu xuất hiện:
{glossary_text}

Đây là một đoạn đối thoại hoặc trần thuật trong tiểu thuyết hư cấu. Hãy chuyển ngữ sang tiếng Việt rõ nghĩa, trung tính, giữ nguyên diễn biến. Chỉ trả về bản dịch.

{text}
```

### `translate-cleanup.txt`
```
{base_rules}
Glossary dùng bắt buộc nếu xuất hiện:
{glossary_text}

Dưới đây là bản dịch tiếng Việt còn lỗi. Hãy chỉ sửa lỗi còn sót: chữ Hán chưa dịch, câu cú gượng, xuống dòng xấu, tiêu đề chương dính hoặc lặp. Không thêm ý mới. Chỉ trả về bản sửa cuối cùng.

{text}
```

### `translate-han-repair-line.txt`
```
{base_rules}
Glossary dùng bắt buộc nếu xuất hiện:
{glossary_text}

Chỉ dịch đúng dòng sau sang tiếng Việt tự nhiên. Nếu dòng chỉ là từ tượng thanh thì dịch thành từ tượng thanh tiếng Việt phù hợp. Chỉ trả về đúng một dòng đã dịch.

{text}
```

### `translate-han-repair-aggressive.txt`
```
{base_rules}
Glossary dùng bắt buộc nếu xuất hiện:
{glossary_text}

Chỉ sửa đoạn văn sau: thay toàn bộ chữ Hán còn sót thành tiếng Việt tự nhiên. Giữ nguyên ý, không thêm bớt. Tuyệt đối không để sót chữ Hán. Chỉ trả về đúng đoạn đã sửa.

{text}
```

### `translate-repair-source.txt`
```
{base_rules}
Dưới đây là bản gốc tiếng Trung và bản dịch tiếng Việt hiện có của cùng một chương.
Nhiệm vụ của ngươi:
- Giữ nguyên toàn bộ nội dung và thứ tự theo bản gốc.
- Chỉ xuất ra bản dịch tiếng Việt cuối cùng của cả chương.
- Phải thay hết toàn bộ chữ Hán còn sót, kể cả chữ Hán lẻ bị trộn trong câu tiếng Việt.
- Nếu bản dịch hiện có đã đúng ở chỗ nào thì giữ nguyên tinh thần, chỉ sửa phần lỗi.
- Không để lại chữ Hán, không giải thích, không ghi chú.

BẢN GỐC:
{source_text}

BẢN DỊCH HIỆN CÓ:
{translated_text}
```

### `translate-repair-placeholders.txt`
```
{base_rules}
Bản dịch tiếng Việt dưới đây đang bị lỗi: còn sót các mã placeholder dạng ZXQ123QXZ hoặc QZX123QXZ.
Nhiệm vụ của ngươi:
- Tuyệt đối không để lại bất kỳ mã ZXQ...QXZ/QZX...QXZ nào trong kết quả.
- Dựa vào bản gốc tiếng Trung để khôi phục đúng tên người/địa danh/tổ chức/chức danh tương ứng.
- Nếu không chắc cách Việt hóa, giữ nguyên chữ Hán của thuật ngữ trong bản gốc (nhưng vẫn không được để token).
- Giữ nguyên nội dung và thứ tự theo bản gốc, không thêm ý, không xóa làm mất nghĩa.
- Chỉ xuất ra bản dịch tiếng Việt cuối cùng của cả đoạn.

PLACEHOLDER ĐANG BỊ LỌT (ví dụ): {examples}

BẢN GỐC:
{source_text}

BẢN DỊCH HIỆN CÓ:
{translated_text}
```

### `translate-glossary-extract.txt`
```
Hãy trích xuất glossary thuật ngữ từ cặp văn bản sau.
Mục tiêu: dùng cho các chương sau của cùng một truyện để giữ cách dịch nhất quán.
Chỉ lấy mục thật sự nên tái sử dụng: tên người, tên trường, địa danh, tổ chức, chức danh riêng, biệt hiệu, thuật ngữ riêng.
Không lấy đại từ, động từ, tính từ, câu hoàn chỉnh, từ thông dụng.
Khóa phải là cụm chữ Hán xuất hiện nguyên văn trong bản gốc. Giá trị phải là đúng cách gọi tiếng Việt đã dùng trong bản dịch.
Giá trị bắt buộc phải xuất hiện nguyên văn trong BẢN DỊCH (copy y nguyên), không được tự bịa hoặc tự suy diễn.
Tuyệt đối không trả về mã placeholder dạng ZXQ123QXZ hoặc QZX123QXZ.
Nếu chưa chắc chắn hoặc bản dịch không thể hiện rõ, bỏ qua.
Ưu tiên cụm dài, tránh tạo mục con dư thừa khi đã có mục dài hơn cùng nghĩa.
Chỉ trả về JSON object thuần, không markdown, không giải thích.

BẢN GỐC{compacted_suffix}:
{compact_source}

BẢN DỊCH{compacted_suffix}:
{compact_translated}
```

`{compacted_suffix}` = `""` hoặc `" (TRÍCH)"` tùy `was_compacted`.

### `caption-translate.txt`
```
Bạn là chuyên gia dịch phụ đề Trung -> Việt.
Dịch tự nhiên theo phong cách phụ đề, ngắn gọn nhưng mượt, ưu tiên câu văn nghe như lời thoại thật.
Tự động thêm dấu câu phù hợp khi cần, đặc biệt là dấu phẩy, dấu chấm, dấu hỏi và dấu chấm than.
Nếu tên riêng hoặc thuật ngữ đã xuất hiện trong glossary bên dưới, giữ nhất quán đúng theo glossary, không tự đổi cách gọi.
Bắt buộc trả về DUY NHẤT JSON object dạng: {"translations":["...", "..."]}.
translations phải có đúng số phần tử như đầu vào, đúng thứ tự.
Dịch toàn bộ sang tiếng Việt, không để lại tiếng Trung, bao gồm cả tên riêng.
Giữ nguyên định dạng subtitle trong dòng nếu có: <i>, </i>, {\an8}, dấu câu, ký hiệu.
{glossary_section}{batch_json}
```

`{glossary_section}` = `""` hoặc `"GLOSSARY:\n{glossary_text}\n"` (code tính trước khi render).

---

## Chi tiết sửa code

### `novel_tts/translate/novel.py`

Thêm import ở đầu file:
```python
from novel_tts.translate.prompts import render_prompt
```

**1. `_safe_literary_prompt()` (dòng 2301–2310)** — xóa hàm, thay bằng:
```python
def _safe_literary_prompt(base_rules: str, glossary_text: str, line_token: str, text: str) -> str:
    return render_prompt(
        "translate-safe-literary.txt",
        base_rules=base_rules,
        glossary_text=glossary_text,
        text=text.replace(chr(10), f" {line_token} "),
    )
```

**2. `_generate_translation_chunk()` — primary_prompt (dòng 2322–2329)**:
```python
primary_prompt = render_prompt(
    "translate-primary.txt",
    base_rules=translation_cfg.base_rules,
    glossary_text=glossary_text,
    text=chunk.replace(chr(10), f" {translation_cfg.line_token} "),
)
```

**3. `_generate_translation_chunk()` — softened_prompt (dòng 2365–2371)**:
```python
softened_prompt = render_prompt(
    "translate-softened.txt",
    base_rules=translation_cfg.base_rules,
    glossary_text=glossary_text,
    text=segment.replace(chr(10), f" {translation_cfg.line_token} "),
)
```

**4. `final_cleanup()` (dòng 2385–2392)**:
```python
prompt = render_prompt(
    "translate-cleanup.txt",
    base_rules=_strip_placeholder_rules(config.translation.base_rules),
    glossary_text=glossary_text,
    text=text,
)
```

**5. `patch_remaining_han()` (dòng 2409–2416)**:
```python
prompt = render_prompt(
    "translate-han-repair-line.txt",
    base_rules=_strip_placeholder_rules(translation_cfg.base_rules),
    glossary_text=glossary_text,
    text=line,
)
```

**6. `aggressive_repair_han()` (dòng 2448–2455)**:
```python
prompt = render_prompt(
    "translate-han-repair-aggressive.txt",
    base_rules=_strip_placeholder_rules(translation_cfg.base_rules),
    glossary_text=glossary_text,
    text=segment,
)
```

**7. `repair_against_source()` (dòng 2473–2484)**:
```python
prompt = render_prompt(
    "translate-repair-source.txt",
    base_rules=_strip_placeholder_rules(config.translation.base_rules),
    source_text=source_text,
    translated_text=translated_text,
)
```

**8. `repair_placeholder_tokens_against_source()` (dòng 2497–2509)**:
```python
prompt = render_prompt(
    "translate-repair-placeholders.txt",
    base_rules=_strip_placeholder_rules(config.translation.base_rules),
    examples=examples,
    source_text=source_text,
    translated_text=translated_text,
)
```

**9. `_extract_glossary_updates()` (dòng 874–886)**:
```python
compacted_suffix = " (TRÍCH)" if was_compacted else ""
prompt = render_prompt(
    "translate-glossary-extract.txt",
    compacted_suffix=compacted_suffix,
    compact_source=compact_source,
    compact_translated=compact_translated,
)
```

### `novel_tts/translate/captions.py`

Thêm import:
```python
from novel_tts.translate.prompts import render_prompt
```

Thay toàn bộ `prompt_parts` list build (dòng 57–71):
```python
glossary_section = f"GLOSSARY:\n{glossary_text}\n" if glossary_text else ""
batch_json = json.dumps({"lines": batch}, ensure_ascii=False)
prompt = render_prompt(
    "caption-translate.txt",
    glossary_section=glossary_section,
    batch_json=batch_json,
)
```

---

## Verification

1. Chạy translate thử 1 chapter nhỏ: `uv run novel-tts translate chapter <novel_id> <chapter>` — xác nhận prompt được load từ file, không lỗi `FileNotFoundError`.
2. Chạy caption translate nếu có: `uv run novel-tts translate captions ...`
3. Sửa thử 1 dòng trong `configs/llm-prompts/translate-primary.txt`, chạy lại — xác nhận thay đổi có hiệu lực ngay (cache `lru_cache` chỉ per-process, restart là reset).
4. `uv run pytest tests/` để đảm bảo không regression.
