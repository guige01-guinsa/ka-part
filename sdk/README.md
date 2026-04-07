# SDK

외부 시스템에서 AI 민원처리 엔진을 붙일 때 사용하는 샘플 클라이언트입니다.

## 인증

- 헤더: `Authorization: Bearer <tenant_api_key>`
- 기본 URL 예시: `https://your-service.example.com/api`

## 포함 파일

- `javascript/client.js`
- `python/client.py`

## 주요 메서드

- `createComplaint`
- `listComplaints`
- `updateComplaint`
- `classify`
- `generateDailyReport`

## 빠른 예시

```javascript
import { ComplaintSDK } from "./javascript/client.js";

const sdk = new ComplaintSDK({
  baseUrl: "https://ka-facility-os.onrender.com/api",
  apiKey: "sk-ka-...",
});

await sdk.createComplaint({
  building: "101",
  unit: "1203",
  channel: "전화",
  content: "엘리베이터가 멈췄어요",
});
```

```python
from client import ComplaintSDK

sdk = ComplaintSDK(
    base_url="https://ka-facility-os.onrender.com/api",
    api_key="sk-ka-...",
)

sdk.create_complaint({
    "building": "101",
    "unit": "1203",
    "channel": "전화",
    "content": "엘리베이터가 멈췄어요",
})
```
