import asyncio
from email.header import decode_header, make_header
from typing import Any, Literal

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
        """Fetch all mail messages except trash/spam for initial sync."""
        headers = {"Authorization": f"Bearer {access_token}"}
        query = "-in:trash -in:spam"

        async with httpx.AsyncClient(timeout=20) as client:
            message_ids = await self._list_all_message_ids(
                client=client,
                headers=headers,
                user_id=user_id,
                query=query,
                page_size=max_results,
            )
            detail_tasks = [
                self._fetch_message_detail(
                    client=client,
                    access_token=access_token,
                    user_id=user_id,
                    message_id=message_id,
                )
                for message_id in message_ids
            ]
            detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

        emails: list[dict[str, Any]] = []
        for result in detail_results:
            if isinstance(result, Exception):
                continue
            emails.append(result)
        return emails

    async def fetch_triage_emails(
        self,
        access_token: str,
        user_id: str = "me",
        max_unread: int = 100,
        max_read: int = 100,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch unread and read buckets for triage."""
        unread_query = "is:unread -in:trash -in:spam"
        read_query = "-is:unread -in:trash -in:spam"

        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient(timeout=20) as client:
            unread_ids = await self._list_all_message_ids(
                client=client,
                headers=headers,
                user_id=user_id,
                query=unread_query,
                page_size=max_unread,
            )
            read_ids = await self._list_all_message_ids(
                client=client,
                headers=headers,
                user_id=user_id,
                query=read_query,
                page_size=max_read,
            )

            all_ids = list(dict.fromkeys(unread_ids + read_ids))
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
        read_messages = [detail_map[mid] for mid in read_ids if mid in detail_map]

        return {
            "unread": unread_messages,
            "read": read_messages,
        }

    async def apply_bulk_action(
        self,
        access_token: str,
        action: str,
        message_ids: list[str],
        user_id: str = "me",
    ) -> dict[str, Any]:
        """Apply archive or trash action to selected messages."""
        if action not in {"archive", "inbox_unlabel", "trash"}:
            raise HTTPException(status_code=400, detail="Unsupported action")

        if action in {"archive", "inbox_unlabel"}:
            await self._batch_unarchive(access_token=access_token, user_id=user_id, message_ids=message_ids)
            return {"processed_count": len(message_ids), "failed_ids": [], "missing_ids": []}

        trash_result = await self._trash_messages(
            access_token=access_token,
            user_id=user_id,
            message_ids=message_ids,
        )
        failed_ids = trash_result["failed_ids"]
        missing_ids = trash_result["missing_ids"]

        # missing_ids(이미 Gmail에서 삭제됨)는 UI/DB 정리 대상으로 간주해 성공 처리한다.
        return {
            "processed_count": len(message_ids) - len(failed_ids),
            "failed_ids": failed_ids,
            "missing_ids": missing_ids,
        }

    async def apply_label_updates(
        self,
        access_token: str,
        user_id: str,
        message_ids: list[str],
        add_label_ids: list[str],
        remove_label_ids: list[str],
    ) -> dict[str, Any]:
        """Apply Gmail label updates in chunks and return processed/failed IDs."""
        remove_label_ids = self._normalize_remove_label_ids(remove_label_ids=remove_label_ids)
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{GMAIL_API_BASE}/users/{user_id}/messages/batchModify"

        failed_ids: list[str] = []
        chunks = [message_ids[i : i + 1000] for i in range(0, len(message_ids), 1000)]
        async with httpx.AsyncClient(timeout=20) as client:
            for chunk in chunks:
                payload = {
                    "ids": chunk,
                    "addLabelIds": add_label_ids,
                    "removeLabelIds": remove_label_ids,
                }
                response = await client.post(url, headers=headers, json=payload)
                if response.status_code >= 400:
                    failed_ids.extend(chunk)

        return {
            "processed_count": len(message_ids) - len(failed_ids),
            "failed_ids": failed_ids,
        }

    async def create_label(
        self,
        access_token: str,
        user_id: str,
        name: str,
    ) -> dict[str, Any]:
        """Create a Gmail user label and return the created label metadata."""
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{GMAIL_API_BASE}/users/{user_id}/labels"
        payload = {
            "name": name.strip(),
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        }

        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(url, headers=headers, json=payload)

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "Failed to create Gmail label",
                    "google_response": response.text,
                },
            )

        data = response.json()
        return {
            "gmail_label_id": data.get("id", ""),
            "name": data.get("name", name.strip()),
            "label_type": data.get("type", "user"),
        }

    @staticmethod
    def _normalize_remove_label_ids(remove_label_ids: list[str]) -> list[str]:
        """라벨 변경 시 받은편지함 해제가 함께 일어나도록 정규화한다."""
        normalized = [str(label).upper() for label in remove_label_ids if str(label).strip()]
        if "INBOX" not in normalized:
            normalized.append("INBOX")
        return list(dict.fromkeys(normalized))

    async def _batch_unarchive(self, access_token: str, user_id: str, message_ids: list[str]) -> None:
        """받은편지함에서 메일을 제거합니다."""
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
                            "message": "Failed to remove inbox label from messages",
                            "google_response": response.text,
                        },
                    )

    async def _trash_messages(
        self,
        access_token: str,
        user_id: str,
        message_ids: list[str],
    ) -> dict[str, list[str]]:
        """Move messages to trash and split failed vs already-missing IDs."""
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
        missing_ids: list[str] = []
        for message_id, result in zip(message_ids, results):
            if isinstance(result, Exception):
                failed_ids.append(message_id)
                continue
            if result == "missing":
                missing_ids.append(message_id)
            elif result == "failed":
                failed_ids.append(message_id)

        return {"failed_ids": failed_ids, "missing_ids": missing_ids}

    async def _trash_one(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        user_id: str,
        message_id: str,
    ) -> Literal["ok", "missing", "failed"]:
        """Move one message to trash and classify outcome."""
        url = f"{GMAIL_API_BASE}/users/{user_id}/messages/{message_id}/trash"
        response = await client.post(url, headers=headers)
        if response.status_code < 400:
            return "ok"
        if response.status_code == 404:
            return "missing"
        return "failed"

    async def _list_all_message_ids(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        user_id: str,
        query: str,
        page_size: int,
    ) -> list[str]:
        """List all message IDs by Gmail search query with pagination."""
        list_url = f"{GMAIL_API_BASE}/users/{user_id}/messages"
        message_ids: list[str] = []
        page_token: str | None = None

        while True:
            params: dict[str, Any] = {"q": query, "maxResults": page_size}
            if page_token:
                params["pageToken"] = page_token

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

            payload = response.json()
            message_refs = payload.get("messages", [])
            message_ids.extend(item["id"] for item in message_refs if "id" in item)

            page_token = payload.get("nextPageToken")
            if not page_token:
                break

        return message_ids

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
