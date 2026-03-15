import asyncio
from email.header import decode_header, make_header
from typing import Any

import httpx
from fastapi import HTTPException, status

GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1"


class GmailService:
    """Service for Gmail message fetch and bulk actions."""

    async def fetch_unread_emails(
        self,
        access_token: str,
        user_id: str = "me",
        max_results: int = 20,
    ) -> list[dict[str, Any]]:
        """Fetch only unread messages."""
        data = await self.fetch_triage_emails(
            access_token=access_token,
            user_id=user_id,
            max_unread=max_results,
            max_stale=1,
        )
        return data["unread"]

    async def fetch_triage_emails(
        self,
        access_token: str,
        user_id: str = "me",
        max_unread: int = 100,
        max_stale: int = 100,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch unread and stale buckets for triage."""
        unread_query = "is:unread -in:trash -in:spam"
        stale_query = "older_than:6m -is:unread -in:trash -in:spam"

        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient(timeout=20) as client:
            unread_ids = await self._list_message_ids(
                client=client,
                headers=headers,
                user_id=user_id,
                query=unread_query,
                max_results=max_unread,
            )
            stale_ids = await self._list_message_ids(
                client=client,
                headers=headers,
                user_id=user_id,
                query=stale_query,
                max_results=max_stale,
            )

            all_ids = list(dict.fromkeys(unread_ids + stale_ids))
            detail_tasks = [
                self._fetch_message_detail(
                    client=client,
                    access_token=access_token,
                    user_id=user_id,
                    message_id=message_id,
                )
                for message_id in all_ids
            ]
            detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

        detail_map: dict[str, dict[str, Any]] = {}
        for result in detail_results:
            if isinstance(result, Exception):
                continue
            detail_map[result.get("gmail_message_id", "")] = result

        unread_messages = [detail_map[mid] for mid in unread_ids if mid in detail_map]
        stale_messages = [detail_map[mid] for mid in stale_ids if mid in detail_map]

        return {
            "unread": unread_messages,
            "stale": stale_messages,
        }

    async def apply_bulk_action(
        self,
        access_token: str,
        action: str,
        message_ids: list[str],
        user_id: str = "me",
    ) -> dict[str, Any]:
        """Apply archive or trash action to selected messages."""
        if action not in {"archive", "trash"}:
            raise HTTPException(status_code=400, detail="Unsupported action")

        if action == "archive":
            await self._batch_archive(access_token=access_token, user_id=user_id, message_ids=message_ids)
            return {"processed_count": len(message_ids), "failed_ids": []}

        failed_ids = await self._trash_messages(
            access_token=access_token,
            user_id=user_id,
            message_ids=message_ids,
        )
        return {
            "processed_count": len(message_ids) - len(failed_ids),
            "failed_ids": failed_ids,
        }

    async def _batch_archive(self, access_token: str, user_id: str, message_ids: list[str]) -> None:
        """Archive messages by removing INBOX label in chunks."""
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{GMAIL_API_BASE}/users/{user_id}/messages/batchModify"

        chunks = [message_ids[i : i + 1000] for i in range(0, len(message_ids), 1000)]
        async with httpx.AsyncClient(timeout=20) as client:
            for chunk in chunks:
                payload = {
                    "ids": chunk,
                    "removeLabelIds": ["INBOX"],
                }
                response = await client.post(url, headers=headers, json=payload)
                if response.status_code >= 400:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "message": "Failed to archive messages",
                            "google_response": response.text,
                        },
                    )

    async def _trash_messages(
        self,
        access_token: str,
        user_id: str,
        message_ids: list[str],
    ) -> list[str]:
        """Move messages to trash and return failed IDs."""
        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient(timeout=20) as client:
            tasks = [
                self._trash_one(
                    client=client,
                    headers=headers,
                    user_id=user_id,
                    message_id=message_id,
                )
                for message_id in message_ids
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_ids: list[str] = []
        for message_id, result in zip(message_ids, results):
            if isinstance(result, Exception):
                failed_ids.append(message_id)
        return failed_ids

    async def _trash_one(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        user_id: str,
        message_id: str,
    ) -> None:
        """Move one message to trash."""
        url = f"{GMAIL_API_BASE}/users/{user_id}/messages/{message_id}/trash"
        response = await client.post(url, headers=headers)
        if response.status_code >= 400:
            raise HTTPException(status_code=400, detail=response.text)

    async def _list_message_ids(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        user_id: str,
        query: str,
        max_results: int,
    ) -> list[str]:
        """List message IDs by Gmail search query."""
        list_url = f"{GMAIL_API_BASE}/users/{user_id}/messages"
        params = {"q": query, "maxResults": max_results}
        response = await client.get(list_url, headers=headers, params=params)
        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "Failed to fetch Gmail list",
                    "google_response": response.text,
                    "query": query,
                },
            )

        message_refs = response.json().get("messages", [])
        return [item["id"] for item in message_refs if "id" in item]

    async def _fetch_message_detail(
        self,
        client: httpx.AsyncClient,
        access_token: str,
        user_id: str,
        message_id: str,
    ) -> dict[str, Any]:
        """Fetch metadata details for a message."""
        url = f"{GMAIL_API_BASE}/users/{user_id}/messages/{message_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        params: list[tuple[str, str]] = [
            ("format", "metadata"),
            ("metadataHeaders", "Subject"),
            ("metadataHeaders", "From"),
            ("metadataHeaders", "To"),
            ("metadataHeaders", "Date"),
        ]
        response = await client.get(url, headers=headers, params=params)
        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": f"Failed to fetch message detail: {message_id}",
                    "google_response": response.text,
                },
            )

        data = response.json()
        headers_map = self._extract_headers(data.get("payload", {}).get("headers", []))
        return {
            "gmail_message_id": data.get("id"),
            "gmail_thread_id": data.get("threadId"),
            "subject": headers_map.get("Subject", ""),
            "from_email": headers_map.get("From", ""),
            "to_email": headers_map.get("To", ""),
            "date_header": headers_map.get("Date", ""),
            "snippet": data.get("snippet", ""),
            "internal_date": data.get("internalDate"),
            "label_ids": data.get("labelIds", []),
            "raw": data,
        }

    @staticmethod
    def _decode_mime_header(value: str) -> str:
        """Decode RFC2047 encoded header text into readable Unicode."""
        if not value:
            return ""
        try:
            return str(make_header(decode_header(value)))
        except Exception:
            return value

    @classmethod
    def _extract_headers(cls, headers: list[dict[str, str]]) -> dict[str, str]:
        """Convert Gmail headers array into decoded dictionary."""
        return {
            item.get("name", ""): cls._decode_mime_header(item.get("value", ""))
            for item in headers
        }


gmail_service = GmailService()