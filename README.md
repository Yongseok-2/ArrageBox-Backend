# 📬 ArrangeBox

ArrangeBox는 단순히 이메일을 지우는 도구를 넘어, 환경을 생각하는 **디지털 지속가능성**을 실천하는 AI 기반 이메일 정리 서비스입니다.

저는 읽지 않은 채 쌓여있는 수만 통의 이메일이 데이터 센터의 에너지를 소모하며 실시간으로 탄소를 배출한다는 점에 주목했습니다. ArrangeBox는 이메일함 구석구석 쌓인 이 메일들을 **탄소 먼지**로 정의하고, 사용자가 이를 정리 바구니에 담아 비워냄으로써 마음의 평화와 환경적 가치를 동시에 얻을 수 있도록 돕습니다.

|List| Repository | GitHub |
|------|------|------|
|ArrangeBox-Frontend| 🟠 **ArrangeBox-Frontend (React)** | [![ArrangeBox-Frontend Repo](https://img.shields.io/static/v1?label=GitHub&message=ArrangeBox-Frontend&color=181717&logo=github&style=flat-square)](https://github.com/Yongseok-2/ArrangeBox-Frontend) |

---

## ✨ Features
| 기능 | 화면 |
|:---:|-----|
| 메인화면 | <img src="https://github.com/user-attachments/assets/b8a51c79-85a4-4bf3-a46b-5db340243298" height="300"/> |
| 이메일 연동 | <img src="https://github.com/user-attachments/assets/07649539-1011-4bb2-84d8-94c24b413953" height="300"/> |
| 정리 바구니에 담기 | <img src="https://github.com/user-attachments/assets/4d532f84-9afa-4485-b2df-710ad6f50a68" height="300"/> |
| 중요 + 별표 작업 | <img src="https://github.com/user-attachments/assets/61ca6b9e-5d0b-4777-9350-807a94e88c33" height="300"/> |
| 삭제 작업 | <img src="https://github.com/user-attachments/assets/d7386b1b-a899-428b-92e2-920bfccfc57a" height="300"/> |

---

## 🛠 Tech Stack
| 구분 | 기술 |
| ------------------------- | -------------------------------- |
| Backend | Python 3.12, FastAPI, Uvicorn |
| Frontend | React (TypeScript), Vite |
| AI API | Gemini API (2.0 Flash-Lite) |
| Auth / External API | Google OAuth2, Gmail API |
| Message Queue | Kafka (KRaft) |
| Database | PostgreSQL |
| Cache / Temporary Storage | Redis |
| Styling / Animations | Tailwind CSS |

---

## 📊 ERD

```mermaid
erDiagram
    EMAIL_ANALYSIS {
        BIGSERIAL id PK
        TEXT account_id
        TEXT gmail_message_id UK
        TEXT gmail_thread_id
        TEXT subject
        TEXT from_email
        TEXT to_email
        TEXT date_header
        TEXT snippet
        TEXT internal_date
        JSONB label_ids
        JSONB payload_json
        VECTOR embedding
        TIMESTAMPTZ processed_at
        TIMESTAMPTZ created_at
        TEXT sender_email
        TEXT category
        INT urgency_score
        TEXT summary
        JSONB keywords
        DOUBLE confidence_score
        TEXT analysis_source
        BOOLEAN review_required
        TEXT draft_reply_context
        TIMESTAMPTZ analyzed_at
    }

    GMAIL_LABELS {
        BIGSERIAL id PK
        TEXT account_id
        TEXT gmail_label_id UK
        TEXT label_name
        TEXT label_type
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    EMAIL_ANALYSIS }o--|| GMAIL_LABELS : "shares account_id"
```

---

## Architecture

