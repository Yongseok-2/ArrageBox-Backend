# 프로젝트 컨텍스트 및 AI 코딩 가이드라인 (CONTEXT.md)

## 1. 프로젝트 개요 (Project Overview)
- **프로젝트명**: InboxZero (가칭)
- **목적**: 사용자의 Gmail을 연동하여, 읽지 않거나 오래된 메일을 분석(Rule-based + AI)하고 대량으로 삭제/보관할 수 있게 돕는 스마트 이메일 트리아지(Triage) API 서버.
- **주요 기술 스택**: 
  - **Language**: Python 3.12.4
  - **Framework**: FastAPI
  - **Database**: PostgreSQL
  - **Message Broker**: Kafka (메일 대량 처리 및 AI 분석 워커용)
  - **AI Model**: Google Gemini 2.5 Flash-Lite
  - **Auth**: Google OAuth 2.0 (Gmail API 연동)

## 2. 디렉토리 구조 및 아키텍처 (Architecture)
코드 수정 및 추가 시 아래의 계층형 아키텍처(Layered Architecture) 규칙을 반드시 준수할 것.

- `app/api/`: 엔드포인트 라우터 (`@router.get` 등). 비즈니스 로직 포함 금지.
- `app/services/`: 핵심 비즈니스 로직 및 외부 API(Gmail, Gemini) 호출 로직.
- `app/models/`: Pydantic 스키마(요청/응답 모델) 및 DB 엔티티.
- `app/core/`: 환경 설정(`settings.py`), 보안, 예외 처리 등 공통 모듈.
- `app/worker/`: Kafka 컨슈머 및 백그라운드 작업 로직.

## 3. 핵심 비즈니스 로직 (Core Logic)
- **메일 분류(Triage) 시스템**:
  1. 모든 메일은 먼저 `_analyze_with_rules` (Rule-based)를 통해 1차 분류 및 점수(Confidence) 산정.
  2. 점수가 Threshold(예: 0.6) 미만이거나 'other' 카테고리인 경우에만 `_analyze_with_gemini` (AI)를 호출하는 하이브리드 비용 절감 로직 유지.
- **일괄 처리(Bulk Action)**: Gmail API 호출 시 건별 처리가 아닌 Batch/Bulk 형태로 처리하여 네트워크 I/O 최소화.

## 4. ⚠️ AI 코딩 규칙 (Strict Conventions) - 반드시 지킬 것

### 4.1. 비동기 프로그래밍 (Async/Await)
- I/O 바운드 작업(DB 통신, 외부 API 호출, 파일 읽기/쓰기)은 **무조건 `async def`와 `await`를 사용**할 것.
- `httpx.AsyncClient` 등을 사용하여 동기적 블로킹(Blocking)이 발생하지 않도록 할 것.

### 4.2. 타입 힌팅 및 검증 (Type Hinting & Validation)
- 모든 함수와 메서드에 100% 파이썬 타입 힌트(Type Hint)를 명시할 것. (예: `def func(a: str) -> int:`)
- API 요청/응답 검증은 반드시 **Pydantic V2** 모델을 사용할 것.

### 4.3. API 문서화 (Swagger/OpenAPI)
- FastAPI 라우터 데코레이터 내부에 `description` 파라미터를 사용하지 말 것.
- 대신 **함수 바로 아래에 `"""독스트링(Docstring)"""`을 마크다운 형식으로 작성**하여 Swagger UI에 노출시킬 것.

### 4.4. 파일 및 환경 설정
- 모든 텍스트/코드 파일은 **UTF-8 (BOM 없음)** 및 **LF(Line Feed)** 줄바꿈 형식으로 저장할 것.
- 민감한 정보(API 키, DB 비밀번호 등)는 하드코딩을 엄격히 금지하며, 반드시 `app.core.settings`를 통해 `.env`에서 불러올 것.

## 5. AI 에이전트 행동 지침 (Agent Instructions)
- 코드를 수정하기 전에 현재 구조가 왜 이렇게 짜여 있는지 먼저 생각(Think Step-by-Step)할 것.
- 요청받은 기능 외에 정상적으로 동작하는 기존 로직(특히 분류 로직이나 라우터)을 임의로 삭제하거나 변경하지 말 것.
- 에러를 수정할 때는 단순히 `try-except`로 덮어씌우지 말고, 근본적인 원인(타입 불일치, 네트워크 타임아웃 등)을 파악하여 방어 코드를 작성할 것.