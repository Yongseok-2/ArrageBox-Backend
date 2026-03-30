import json
import logging
from datetime import UTC, datetime
from typing import Any

import httpx

from app.core.settings import settings


logger = logging.getLogger(__name__)

ALLOWED_CATEGORIES = {
    "work_action",
    "finance_billing",
    "account_security",
    "shopping_delivery",
    "newsletter_promo",
    "social_community",
    "personal",
    "travel_reservation",
    "career_recruitment",
    "education_learning",
    "other",
}

SENDER_PRIORS: dict[str, tuple[str, float]] = {
    # --- 기존 항목 ---
    "account_noreply@navercorp.com": ("account_security", 0.35),
    "no-reply@accounts.google.com": ("account_security", 0.35),
    "verify@twitter.com": ("account_security", 0.30),
    "mail@musinsa.com": ("shopping_delivery", 0.25),
    "member-cs@musinsa.com": ("shopping_delivery", 0.25),
    "noreply@po.atlassian.net": ("work_action", 0.20),
    "jira@": ("work_action", 0.20),
    "confluence@": ("work_action", 0.20),
    "pinterest": ("newsletter_promo", 0.25),
    "news@": ("newsletter_promo", 0.20),
    "newsletter": ("newsletter_promo", 0.20),
    "discord.com": ("social_community", 0.25),
    "facebookmail.com": ("social_community", 0.25),

    # --- [추가] 계정 보안 (Account Security) ---
    "kakaocorp.com": ("account_security", 0.30),
    "samsung_account": ("account_security", 0.30),
    "appleid@id.apple.com": ("account_security", 0.35),
    "login@": ("account_security", 0.25),
    "security@": ("account_security", 0.25),

    # --- [추가] 쇼핑 및 배송 (Shopping & Delivery) ---
    "coupang.com": ("shopping_delivery", 0.30),
    "29cm.co.kr": ("shopping_delivery", 0.30),
    "marketkurly.com": ("shopping_delivery", 0.25),
    "shipping@": ("shopping_delivery", 0.25),
    "order@": ("shopping_delivery", 0.25),
    "delivery@": ("shopping_delivery", 0.25),

    # --- [추가] 금융 및 청구 (Finance & Billing) ---
    "shinhancard.com": ("finance_billing", 0.30),
    "samsungcard": ("finance_billing", 0.30),
    "hyundaicard": ("finance_billing", 0.30),
    "kbcard.com": ("finance_billing", 0.30),
    "kakaobank.com": ("finance_billing", 0.30),
    "toss.im": ("finance_billing", 0.25),
    "nts.go.kr": ("finance_billing", 0.35), # 국세청
    "bill@": ("finance_billing", 0.25),
    "invoice@": ("finance_billing", 0.25),

    # --- [신규] 여행 및 예약 (Travel & Reservation) ---
    "korail.com": ("travel_reservation", 0.35), # KTX
    "flyasiana.com": ("travel_reservation", 0.35),
    "koreanair.com": ("travel_reservation", 0.35),
    "agoda.com": ("travel_reservation", 0.30),
    "booking.com": ("travel_reservation", 0.30),
    "airbnb.com": ("travel_reservation", 0.30),
    "reservation@": ("travel_reservation", 0.25),
    "booking@": ("travel_reservation", 0.25),

    # --- [신규] 채용 및 커리어 (Career & Recruitment) ---
    "saramin.co.kr": ("career_recruitment", 0.30),
    "jobkorea.co.kr": ("career_recruitment", 0.30),
    "wanted.co.kr": ("career_recruitment", 0.30),
    "linkedin.com": ("career_recruitment", 0.25),
    "recruit@": ("career_recruitment", 0.35),
    "hr@": ("career_recruitment", 0.35),
    "hiring@": ("career_recruitment", 0.30),

    # --- [신규] 교육 및 학업 (Education & Learning) ---
    "inflearn.com": ("education_learning", 0.30),
    "fastcampus.co.kr": ("education_learning", 0.30),
    "udemy.com": ("education_learning", 0.25),
    "coursera.org": ("education_learning", 0.25),
    "edu@": ("education_learning", 0.30),
    "university": ("education_learning", 0.20),
    "ac.kr": ("education_learning", 0.25), # 대학교 도메인

    # --- [추가] 업무용 도구 (Work Action) ---
    "slack.com": ("work_action", 0.25),
    "notion.so": ("work_action", 0.25),
    "zoom.us": ("work_action", 0.20),
    "microsoft.com": ("work_action", 0.15),
    "google.com/calendar": ("work_action", 0.30),

    # --- [추가] 소셜 (Social Community) ---
    "instagram.com": ("social_community", 0.25),
    "youtube.com": ("social_community", 0.20),
    "twitter.com": ("social_community", 0.20),
}


class EmailAnalyzer:
    """Analyze email category, urgency, and summary."""

    def analyze_email_rules(self, email: dict[str, Any]) -> dict[str, Any]:
        """Run rule-based analysis only."""
        message_id = str(email.get("gmail_message_id") or "")
        subject = str(email.get("subject") or "")
        from_email = str(email.get("from_email") or "")
        logger.info(
            "Analyze email start: message_id=%s subject=%s from=%s",
            message_id,
            subject[:80],
            from_email[:80],
        )

        rule_result = self._analyze_with_rules(email)
        logger.debug(
            "Rule analysis result: message_id=%s source=%s category=%s confidence=%.4f review_required=%s",
            message_id,
            rule_result["analysis_source"],
            rule_result["category"],
            float(rule_result["confidence_score"]),
            bool(rule_result["review_required"]),
        )
        return rule_result

    async def analyze_email(self, email: dict[str, Any]) -> dict[str, Any]:
        """Run rule-based analysis and fallback to Gemini for ambiguous cases."""
        message_id = str(email.get("gmail_message_id") or "")
        rule_result = self.analyze_email_rules(email)
        if not self._is_ambiguous(rule_result):
            logger.info(
                "Analyze email done by rules: message_id=%s category=%s confidence=%.4f",
                message_id,
                rule_result["category"],
                float(rule_result["confidence_score"]),
            )
            return rule_result

        if not settings.gemini_enabled:
            logger.info(
                "Rule analysis ambiguous, Gemini disabled: message_id=%s category=%s confidence=%.4f",
                message_id,
                rule_result["category"],
                float(rule_result["confidence_score"]),
            )
            rule_result["review_required"] = True
            return rule_result

        logger.info(
            "Rule analysis ambiguous, trying Gemini: message_id=%s category=%s confidence=%.4f",
            message_id,
            rule_result["category"],
            float(rule_result["confidence_score"]),
        )
        gemini_result = await self._analyze_with_gemini(email, rule_result)
        if gemini_result is not None:
            logger.info(
                "Analyze email done by Gemini: message_id=%s category=%s confidence=%.4f",
                message_id,
                gemini_result["category"],
                float(gemini_result["confidence_score"]),
            )
            return gemini_result

        rule_result["review_required"] = True
        return rule_result

    async def analyze_other_emails_with_gemini(
        self,
        emails: list[dict[str, Any]],
        fallbacks: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Batch re-analyze 'other' emails with Gemini and return results by message ID."""
        if not emails or not settings.gemini_enabled or not settings.gemini_api_key:
            return {}

        prompt = self._build_gemini_batch_prompt(emails=emails, fallbacks=fallbacks)
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{settings.gemini_model}:generateContent?key={settings.gemini_api_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }

        try:
            async with httpx.AsyncClient(timeout=settings.gemini_timeout_seconds) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "Gemini batch request failed: model=%s batch_size=%s status=%s body=%s",
                settings.gemini_model,
                len(emails),
                exc.response.status_code,
                exc.response.text[:1000],
            )
            return {}
        except httpx.HTTPError as exc:
            logger.warning(
                "Gemini batch request failed: model=%s batch_size=%s error=%s",
                settings.gemini_model,
                len(emails),
                str(exc),
            )
            return {}

        parsed_items = self._parse_gemini_batch_json(response.json())
        if parsed_items is None:
            logger.warning(
                "Gemini batch response parse failed: model=%s batch_size=%s",
                settings.gemini_model,
                len(emails),
            )
            return {}

        fallback_map = {
            str(fallback.get("gmail_message_id", "")): fallback for fallback in fallbacks
        }
        results: dict[str, dict[str, Any]] = {}
        for item in parsed_items:
            message_id = str(item.get("gmail_message_id", "")).strip()
            if not message_id or message_id not in fallback_map:
                continue
            merged = self._merge_gemini_result(item=item, fallback=fallback_map[message_id])
            results[message_id] = merged
        return results

    def _analyze_with_rules(self, email: dict[str, Any]) -> dict[str, Any]:
        """Perform fast rule-based analysis."""
        subject = (email.get("subject") or "").strip()
        snippet = (email.get("snippet") or "").strip()
        from_email = (email.get("from_email") or "").strip().lower()
        text = f"{subject} {snippet}".lower()

        category, confidence = self._classify_category(text=text, from_email=from_email)
        urgency_score = self._score_urgency(text=text)
        keywords = self._extract_keywords(text=text)
        summary = self._build_summary(subject=subject, snippet=snippet)

        logger.debug(
            "Rules analyzed: message_id=%s category=%s confidence=%.4f urgency=%s keywords=%s",
            email.get("gmail_message_id"),
            category,
            confidence,
            urgency_score,
            keywords,
        )

        return {
            "gmail_message_id": email.get("gmail_message_id"),
            "sender_email": email.get("from_email", ""),
            "category": category,
            "urgency_score": urgency_score,
            "summary": summary,
            "keywords": keywords,
            "confidence_score": confidence,
            "analysis_source": "rules",
            "review_required": confidence < settings.analysis_confidence_threshold,
            "draft_reply_context": self._build_draft_context(category=category, summary=summary),
            "analyzed_at": datetime.now(UTC),
        }

    def _is_ambiguous(self, result: dict[str, Any]) -> bool:
        """Return True for low-confidence or unknown category result."""
        return bool(
            result["confidence_score"] < settings.analysis_confidence_threshold
            or result["category"] == "other"
        )

    async def _analyze_with_gemini(
        self, email: dict[str, Any], fallback: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Re-analyze ambiguous cases with Gemini model."""
        if not settings.gemini_api_key:
            return None

        prompt = self._build_gemini_prompt(email, fallback)
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{settings.gemini_model}:generateContent?key={settings.gemini_api_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }

        try:
            async with httpx.AsyncClient(timeout=settings.gemini_timeout_seconds) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
        except httpx.HTTPError:
            logger.warning(
                "Gemini request failed: message_id=%s model=%s",
                fallback.get("gmail_message_id"),
                settings.gemini_model,
            )
            return None

        data = response.json()
        parsed = self._parse_gemini_json(data)
        if parsed is None:
            logger.warning(
                "Gemini response parse failed: message_id=%s model=%s",
                fallback.get("gmail_message_id"),
                settings.gemini_model,
            )
            return None

        category = parsed.get("category", fallback["category"])
        if category not in ALLOWED_CATEGORIES:
            category = "other"

        urgency_score = int(parsed.get("urgency_score", fallback["urgency_score"]))
        confidence_score = float(parsed.get("confidence_score", fallback["confidence_score"]))
        summary = str(parsed.get("summary", fallback["summary"]))[:240]
        keywords = parsed.get("keywords", fallback["keywords"])
        if not isinstance(keywords, list):
            keywords = fallback["keywords"]

        logger.debug(
            "Gemini parsed: message_id=%s category=%s confidence=%.4f urgency=%s keywords=%s",
            fallback.get("gmail_message_id"),
            category,
            confidence_score,
            urgency_score,
            keywords,
        )

        return {
            "gmail_message_id": fallback["gmail_message_id"],
            "sender_email": fallback["sender_email"],
            "category": category,
            "urgency_score": max(0, min(100, urgency_score)),
            "summary": summary,
            "keywords": [str(item)[:50] for item in keywords[:12]],
            "confidence_score": max(0.0, min(1.0, confidence_score)),
            "analysis_source": "gemini_flash_lite",
            "review_required": confidence_score < settings.analysis_confidence_threshold,
            "draft_reply_context": self._build_draft_context(category=category, summary=summary),
            "analyzed_at": datetime.now(UTC),
        }

    def _parse_gemini_json(self, response_json: dict[str, Any]) -> dict[str, Any] | None:
        """Parse JSON text from Gemini response."""
        try:
            candidates = response_json.get("candidates", [])
            if not candidates:
                return None
            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                return None
            text = parts[0].get("text", "")
            return json.loads(text)
        except (ValueError, KeyError, TypeError):
            return None

    def _parse_gemini_batch_json(self, response_json: dict[str, Any]) -> list[dict[str, Any]] | None:
        """Parse JSON array text from Gemini batch response."""
        parsed = self._parse_gemini_json(response_json)
        if not isinstance(parsed, list):
            return None
        return [item for item in parsed if isinstance(item, dict)]

    def _build_gemini_prompt(self, email: dict[str, Any], fallback: dict[str, Any]) -> str:
        """Build prompt for Gemini classification request."""
        logger.debug(
            "Gemini prompt prepared: message_id=%s subject=%s from=%s fallback_category=%s fallback_confidence=%.4f",
            email.get("gmail_message_id"),
            str(email.get("subject") or "")[:80],
            str(email.get("from_email") or "")[:80],
            fallback["category"],
            float(fallback["confidence_score"]),
        )
        return (
            "You are classifying a Gmail message for inbox triage.\n"
            "Return JSON only with fields: category, urgency_score, summary, keywords, confidence_score.\n"
            f"Allowed category values: {sorted(ALLOWED_CATEGORIES)}\n"
            "urgency_score must be integer 0..100. confidence_score must be float 0..1.\n"
            "If uncertain, use category 'other' and lower confidence_score.\n\n"
            f"from_email: {email.get('from_email', '')}\n"
            f"subject: {email.get('subject', '')}\n"
            f"snippet: {email.get('snippet', '')}\n"
            f"rule_based_hint: category={fallback['category']}, confidence={fallback['confidence_score']}\n"
        )

    def _build_gemini_batch_prompt(
        self,
        emails: list[dict[str, Any]],
        fallbacks: list[dict[str, Any]],
    ) -> str:
        """Build prompt for Gemini batch classification."""
        items: list[dict[str, Any]] = []
        for email, fallback in zip(emails, fallbacks):
            items.append(
                {
                    "gmail_message_id": str(email.get("gmail_message_id", "")),
                    "from_email": str(email.get("from_email", "")),
                    "subject": str(email.get("subject", "")),
                    "snippet": str(email.get("snippet", "")),
                    "rule_based_hint": {
                        "category": str(fallback.get("category", "other")),
                        "confidence_score": float(fallback.get("confidence_score", 0.0)),
                    },
                }
            )
        return (
            "You are classifying Gmail messages for inbox triage.\n"
            "Return JSON only.\n"
            "Return a JSON array with one object per input email.\n"
            "Each object must contain: gmail_message_id, category, urgency_score, summary, keywords, confidence_score.\n"
            f"Allowed category values: {sorted(ALLOWED_CATEGORIES)}\n"
            "urgency_score must be integer 0..100. confidence_score must be float 0..1.\n"
            "Preserve each gmail_message_id exactly as given.\n"
            "If uncertain, use category 'other' and lower confidence_score.\n\n"
            f"emails: {json.dumps(items, ensure_ascii=True)}\n"
        )

    def _merge_gemini_result(
        self,
        item: dict[str, Any],
        fallback: dict[str, Any],
    ) -> dict[str, Any]:
        """Merge one Gemini result object with a fallback rule result."""
        category = str(item.get("category", fallback["category"]))
        if category not in ALLOWED_CATEGORIES:
            category = "other"

        raw_keywords = item.get("keywords", fallback["keywords"])
        keywords = raw_keywords if isinstance(raw_keywords, list) else fallback["keywords"]
        summary = str(item.get("summary", fallback["summary"]))[:240]
        urgency_score = int(item.get("urgency_score", fallback["urgency_score"]))
        confidence_score = float(item.get("confidence_score", fallback["confidence_score"]))

        return {
            "gmail_message_id": fallback["gmail_message_id"],
            "sender_email": fallback["sender_email"],
            "category": category,
            "urgency_score": max(0, min(100, urgency_score)),
            "summary": summary,
            "keywords": [str(keyword)[:50] for keyword in keywords[:12]],
            "confidence_score": max(0.0, min(1.0, confidence_score)),
            "analysis_source": "gemini_flash",
            "review_required": confidence_score < settings.analysis_confidence_threshold,
            "draft_reply_context": self._build_draft_context(category=category, summary=summary),
            "analyzed_at": datetime.now(UTC),
        }

    def _classify_category(self, text: str, from_email: str) -> tuple[str, float]:
        """Classify category with keyword scores + sender prior."""
        scores = {category: 0.0 for category in ALLOWED_CATEGORIES}

        keyword_rules: dict[str, list[str]] = {
    "finance_billing": [
        "invoice", "receipt", "billing", "payment", "refund", "statement", "tax", "banking",
        "credit card", "premium", "subscription", "automatic transfer", "overdue", "wire transfer",
        "charge", "settlement", "claim", "transfer", "installment", "deposit", "withdrawal",
        "cash receipt", "amount", "notice",
        "청구서", "영수증", "결제", "납부", "환불", "명세서", "세금", "은행", "카드", "보험료", 
        "구독", "자동이체", "미납", "연체", "송금", "입금", "출금", "정산", "현금영수증", "금액"
    ],
    "work_action": [
        "meeting", "schedule", "calendar", "action required", "jira", "confluence", "slack", "zoom",
        "resume", "interview", "hcm", "workday", "pr", "mr", "ticket", "approval", "urgent",
        "deadline", "review", "submission", "application", "feedback", "request", "approve", 
        "reject", "submit", "report", "task", "share", "plan", "work", "discussion", "notice",
        "회의", "일정", "조치", "업무", "협업", "이력서", "면접", "인터뷰", "승인", "반려", 
        "검토", "제출", "마감", "기한", "보고서", "공유", "협의", "알림", "통지", "기획"
    ],
    "account_security": [
        "security", "verify", "password", "login alert", "authentication", "otp", "mfa", "2fa",
        "recovery", "reset", "suspicious", "blocked", "login attempt", "device", "ip", "unauthorized",
        "auth", "login", "code", "block", "access", "alert", "change",
        "보안", "인증", "비밀번호", "로그인", "확인", "재설정", "차단", "의심", "기기", "접속", "알림", "경고", "해지"
    ],
    "shopping_delivery": [
        "shipping", "delivered", "order", "tracking", "shipment", "dispatch", "out of stock",
        "return", "cancellation", "courier", "logistics", "fedex", "ups", "dhl", "tracking number",
        "purchase", "delivery", "shipping complete", "pickup", "buy", "shopping",
        "sold out", "cancel", "parcel", "warehouse", "exchange",
        "배송", "주문", "완료", "출고", "택배", "송장", "구매", "취소", "환불", "반품", "교환", "품절", "화물", "수령"
    ],
    "travel_reservation": [
        "flight", "hotel", "booking", "reservation", "itinerary", "boarding pass", "check-in", "stay",
        "accommodation", "rental car", "trip", "travel",
        "항공", "숙박", "예약", "여정", "탑승권", "체크인", "숙소", "열차", "코레일", "비행기", "호텔", "펜션"
    ],
    "career_recruitment": [
        "interview", "recruitment", "application", "offer", "hiring", "resume", "job description", "hr", "position",
        "headhunter", "job opening", "合格",
        "채용", "면접", "서류", "합격", "불합격", "입사", "헤드헌터", "공고", "지원서", "구인", "채용절차"
    ],
    "education_learning": [
        "course", "lecture", "enrollment", "exam", "certificate", "university", "webinar", "assignment",
        "academy", "curriculum", "grade",
        "강의", "수강", "자격증", "시험", "성적", "교육", "과제", "세미나", "컨퍼런스", "학원", "대학교", "동영상강의"
    ],
    "newsletter_promo": [
        "sale", "discount", "promotion", "unsubscribe", "newsletter", "coupon", "benefit",
        "offer", "limited", "ad", "marketing", "survey", "webinar", "free", "membership", "loyalty",
        "promo", "deal", "subscribe", "event", "guide",
        "할인", "세일", "특가", "이벤트", "쿠폰", "혜택", "뉴스레터", "광고", "마케팅", "홍보", "무료", "멤버십", "안내", "가이드"
    ],
    "social_community": [
        "facebook", "discord", "community", "follower", "mention", "tag", "invite", "comment",
        "message", "notice", "notification", "thread", "sns", "youtube", "instagram", "reddit",
        "reply", "social", "post", "announcement", "forum", "band",
        "커뮤니티", "팔로우", "언급", "태그", "초대", "댓글", "메시지", "공지", "알림", "게시글", "스레드", "답장"
    ],
    "personal": [
        "mom", "dad", "family", "friend", "brother", "sister", "wedding", "invitation",
        "congratulations", "birthday", "lunch", "dinner", "rsvp", "trip", "vacation",
        "personal", "greeting", "party", "travel", "holiday",
        "가족", "친구", "결혼", "청첩장", "축하", "생일", "여행", "휴가", "모임", "인사", "점심", "저녁"
    ],
}

        for category, words in keyword_rules.items():
            for word in words:
                if word in text:
                    scores[category] += 0.20

        lowered_sender = from_email.lower()
        for sender_hint, (category, weight) in SENDER_PRIORS.items():
            if sender_hint in lowered_sender:
                scores[category] += weight

        best_category = "other"
        best_score = 0.0
        for category, score in scores.items():
            if category == "other":
                continue
            if score > best_score:
                best_category = category
                best_score = score

        if best_score < 0.20:
            return ("other", 0.45)

        confidence = min(0.95, 0.55 + best_score * 0.25)
        return (best_category, round(confidence, 4))

    def _score_urgency(self, text: str) -> int:
        """Compute urgency score between 0 and 100."""
        score = 20
        urgent_keywords = [
            "urgent", "asap", "immediately", "today", "deadline", "overdue", "final notice", "action required",
            "critical", "now", "soon", "alert", "important", "required",
        ]
        for word in urgent_keywords:
            if word in text:
                score += 12
        return min(score, 100)

    def _extract_keywords(self, text: str) -> list[str]:
        """Extract a small keyword set for UI and filtering."""
        candidate_words = [
            "invoice", "payment", "meeting", "deadline", "urgent", "promotion", "receipt", "shipping", "security",
            "verification", "refund", "tracking", "login", "password", "order", "delivery", "sale", "event",
        ]
        return [word for word in candidate_words if word in text]

    def _build_summary(self, subject: str, snippet: str) -> str:
        """Build short summary from subject/snippet."""
        if subject and snippet:
            return f"{subject} - {snippet[:100]}"
        if subject:
            return subject[:120]
        return snippet[:120]

    def _build_draft_context(self, category: str, summary: str) -> str:
        """Build compact context string for draft generation."""
        return f"category={category}; summary={summary}"


email_analyzer = EmailAnalyzer()
