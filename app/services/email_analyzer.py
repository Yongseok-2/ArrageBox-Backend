import json
from datetime import UTC, datetime
from typing import Any

import httpx

from app.core.settings import settings

ALLOWED_CATEGORIES = {
    "work_action",
    "finance_billing",
    "account_security",
    "shopping_delivery",
    "newsletter_promo",
    "social_community",
    "personal",
    "other",
}

SENDER_PRIORS: dict[str, tuple[str, float]] = {
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
}


class EmailAnalyzer:
    """Analyze email category, urgency, and summary."""

    async def analyze_email(self, email: dict[str, Any]) -> dict[str, Any]:
        """Run rule-based analysis and fallback to Gemini for ambiguous cases."""
        rule_result = self._analyze_with_rules(email)
        if not self._is_ambiguous(rule_result):
            return rule_result

        gemini_result = await self._analyze_with_gemini(email, rule_result)
        if gemini_result is not None:
            return gemini_result

        rule_result["review_required"] = True
        return rule_result

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
            return None

        data = response.json()
        parsed = self._parse_gemini_json(data)
        if parsed is None:
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

    def _build_gemini_prompt(self, email: dict[str, Any], fallback: dict[str, Any]) -> str:
        """Build prompt for Gemini classification request."""
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

    def _classify_category(self, text: str, from_email: str) -> tuple[str, float]:
        """Classify category with keyword scores + sender prior."""
        scores = {category: 0.0 for category in ALLOWED_CATEGORIES}

        keyword_rules: dict[str, list[str]] = {
            "finance_billing": [
                "invoice", "receipt", "billing", "payment", "refund", "영수증", "결제", "청구", "환불", "정산",
            ],
            "work_action": [
                "meeting", "schedule", "calendar", "action required", "jira", "confluence", "회의", "일정", "요청", "조치", "승인",
            ],
            "account_security": [
                "security", "verify", "password", "login alert", "authentication", "otp", "인증", "보안", "로그인", "비밀번호", "인증번호", "의심",
            ],
            "shopping_delivery": [
                "shipping", "delivered", "order", "tracking", "주문", "배송", "출고", "택배", "운송장", "구매",
            ],
            "newsletter_promo": [
                "sale", "discount", "promotion", "unsubscribe", "newsletter", "광고", "할인", "이벤트", "뉴스레터", "구독해지",
            ],
            "social_community": [
                "facebook", "discord", "community", "follower", "댓글", "커뮤니티", "알림", "소셜",
            ],
            "personal": [
                "mom", "dad", "family", "friend", "개인", "안부", "지인", "친구", "가족",
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
            "긴급", "즉시", "오늘", "마감", "기한", "지연", "필수 조치",
        ]
        for word in urgent_keywords:
            if word in text:
                score += 12
        return min(score, 100)

    def _extract_keywords(self, text: str) -> list[str]:
        """Extract a small keyword set for UI and filtering."""
        candidate_words = [
            "invoice", "payment", "meeting", "deadline", "urgent", "promotion", "receipt", "shipping", "security",
            "verification", "refund", "tracking", "인증", "보안", "결제", "청구", "배송", "주문", "할인", "이벤트",
        ]
        return [word for word in candidate_words if word in text]

    def _build_summary(self, subject: str, snippet: str) -> str:
        """Build short summary from subject/snippet."""
        if subject and snippet:
            return f"{subject} - {snippet[:120]}"
        if subject:
            return subject[:160]
        return snippet[:160]

    def _build_draft_context(self, category: str, summary: str) -> str:
        """Build compact context string for draft generation."""
        return f"category={category}; summary={summary}"


email_analyzer = EmailAnalyzer()