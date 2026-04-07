from __future__ import annotations

from typing import Any, Dict, Optional

import requests


class ComplaintEngineClient:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.api_key = str(api_key or "").strip()

    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, json: Optional[Dict[str, Any]] = None) -> Any:
        response = requests.request(
            method,
            f"{self.base_url}{path}",
            params=params,
            json=json,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def create_complaint(self, data: Dict[str, Any]) -> Any:
        return self._request("POST", "/complaints", json=data)

    def list_complaints(self, **params: Any) -> Any:
        return self._request("GET", "/complaints", params=params)

    def update_complaint(self, complaint_id: int, data: Dict[str, Any]) -> Any:
        return self._request("PUT", f"/complaints/{complaint_id}", json=data)

    def classify(self, text: str) -> Any:
        return self._request("POST", "/ai/classify", json={"text": text})

    def generate_daily_report(self, **params: Any) -> Any:
        return self._request("GET", "/report/daily", params=params)


if __name__ == "__main__":
    client = ComplaintEngineClient(
        base_url="https://your-service.example.com/api",
        api_key="sk-ka-...",
    )
    print(
        client.create_complaint(
            {
                "building": "101",
                "unit": "1203",
                "channel": "전화",
                "content": "엘리베이터가 멈췄어요",
            }
        )
    )
