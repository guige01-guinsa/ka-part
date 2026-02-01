# 카카오 알림톡 연동 설정

## 1) DB 마이그레이션
```
sqlite3 c:\ka-part\ka.db < c:\ka-part\sql\migrations\20260201_notification_templates.sql
```

## 2) 환경 변수 (BizMessage 방식)
```
KAKAO_API_URL=...         # 업체 제공 URL
KAKAO_AUTH_TOKEN=...      # Bearer 토큰
KAKAO_SENDER_KEY=...      # 발신 프로필 키
```

## 3) 템플릿 등록 (관리 UI 또는 API)
- 이벤트 키 예시: `COMPLAINT_NEW`, `WORK_STATUS`, `WORK_OUTSOURCING`
- API: `POST /api/admin/notification-templates`
```
{
  "event_key": "COMPLAINT_NEW",
  "template_code": "TMPL_001",
  "enabled": 1,
  "message_format": "{title}\n{message}\n작업ID:{work_id}"
}
```

## 4) 동작 방식
- 템플릿이 있으면 `template_code`로 BizMessage 호출
- 템플릿이 없으면 큐에 적재 (notification_queue)

## 5) 수신 대상
- 기본: 소장/관리자 + 시설과장 + 담당자
- 외주 지정 시: 해당 외주업체 사용자도 추가 전송

## 6) 전화번호
- users.phone 값이 있어야 발송됩니다.
