from __future__ import annotations

import json
from typing import Any, Dict, List

COMMON_DOCUMENT_FIELD_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "key": "target_label",
        "label": "대상/설비/문서대상",
        "type": "text",
        "placeholder": "예: 상가A동 25호기 승강기",
    },
    {
        "key": "vendor_name",
        "label": "업체/상대처",
        "type": "text",
        "placeholder": "예: 한국미쓰비시엘리베이터(주)",
    },
    {
        "key": "amount_total",
        "label": "금액(원)",
        "type": "number",
        "placeholder": "예: 298320",
    },
    {
        "key": "basis_date",
        "label": "기준일",
        "type": "date",
        "placeholder": "",
    },
    {
        "key": "period_start",
        "label": "시작일",
        "type": "date",
        "placeholder": "",
    },
    {
        "key": "period_end",
        "label": "종료일",
        "type": "date",
        "placeholder": "",
    },
]

DOCUMENT_CATEGORY_PROFILES: List[Dict[str, Any]] = [
    {
        "category": "기안지(10만원 이상)",
        "code": "DRF",
        "pdf_heading": "기 안 지",
        "default_title": "집행 기안의 건",
        "description": "10만원 이상 집행·교체·지급 건을 결재 상신하는 문서입니다.",
        "amount_policy": "10만원 이상 결재 대상",
        "summary_placeholder": "집행 사유, 작업 내역, 금액, 첨부 문서를 간단히 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "amount_total", "basis_date"],
        "request_text": "위 사항과 같이 집행 결재를 요청드립니다.",
    },
    {
        "category": "구매요청서(10만원 이하)",
        "code": "PUR",
        "pdf_heading": "구 매 요 청 서",
        "default_title": "소액 구매 요청의 건",
        "description": "10만원 이하 소액 구매나 부자재 구입을 요청하는 문서입니다.",
        "amount_policy": "10만원 이하 소액 구매",
        "summary_placeholder": "구매 품목, 필요 사유, 예상 금액을 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "amount_total", "basis_date"],
        "request_text": "위 사항과 같이 소액 구매를 요청드립니다.",
    },
    {
        "category": "견적서와 발주서",
        "code": "ORD",
        "pdf_heading": "견적 및 발주 문서",
        "default_title": "견적 검토 및 발주의 건",
        "description": "견적 비교와 발주 승인 내용을 함께 관리하는 문서입니다.",
        "amount_policy": "견적 검토 후 발주 승인",
        "summary_placeholder": "견적 비교 결과와 발주 사유를 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "amount_total", "basis_date"],
        "request_text": "위 사항과 같이 견적 검토 후 발주 승인을 요청드립니다.",
    },
    {
        "category": "월업무보고(작업 보고서)",
        "code": "MWR",
        "pdf_heading": "월 업 무 보 고",
        "default_title": "월간 업무보고",
        "description": "월간 수행 업무와 작업 결과를 보고하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "월간 주요 업무, 처리 건수, 특이사항을 입력하세요.",
        "focus_fields": ["target_label", "basis_date", "period_start", "period_end"],
        "request_text": "월간 업무 및 작업 결과를 보고드립니다.",
    },
    {
        "category": "계약서관리",
        "code": "CTR",
        "pdf_heading": "계 약 서 관 리",
        "default_title": "계약 관리 기록",
        "description": "계약 체결, 갱신, 종료, 보관 내역을 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "계약 목적, 주요 조건, 갱신 필요사항을 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "amount_total", "period_start", "period_end"],
        "request_text": "계약 관리 내역을 보고드립니다.",
    },
    {
        "category": "배상보험",
        "code": "LIA",
        "pdf_heading": "배 상 보 험 관 리",
        "default_title": "배상보험 관리",
        "description": "배상보험 가입, 갱신, 증권 관리 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "보험 범위, 보장 내용, 갱신 일정을 입력하세요.",
        "focus_fields": ["vendor_name", "amount_total", "period_start", "period_end"],
        "request_text": "배상보험 관리 사항을 보고드립니다.",
    },
    {
        "category": "주요업무일정관리",
        "code": "SCH",
        "pdf_heading": "주요 업무 일정 관리",
        "default_title": "주요 업무 일정 관리",
        "description": "회의, 보고, 점검 등 주요 업무 일정을 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "주요 일정, 담당자, 준비사항을 입력하세요.",
        "focus_fields": ["target_label", "basis_date", "period_start", "period_end"],
        "request_text": "주요 업무 일정을 보고드립니다.",
    },
    {
        "category": "전기수도검침",
        "code": "MTR",
        "pdf_heading": "전기·수도 검침",
        "default_title": "전기수도 검침 기록",
        "description": "전기와 수도 검침 결과를 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "검침 월, 사용량 요약, 특이사항을 입력하세요.",
        "focus_fields": ["target_label", "basis_date", "period_start", "period_end"],
        "request_text": "전기·수도 검침 결과를 보고드립니다.",
    },
    {
        "category": "전기수도부과",
        "code": "BIL",
        "pdf_heading": "전기·수도 부과",
        "default_title": "전기수도 부과 내역",
        "description": "전기와 수도 부과 기준 및 금액 산정을 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "부과 기준, 단가, 부과 대상, 특이사항을 입력하세요.",
        "focus_fields": ["target_label", "amount_total", "basis_date", "period_start", "period_end"],
        "request_text": "전기·수도 부과 내역을 보고드립니다.",
    },
    {
        "category": "직무고시",
        "code": "DUTY",
        "pdf_heading": "직 무 고 시",
        "default_title": "직무고시 관리",
        "description": "직무고시 대상 업무와 점검 항목을 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "직무고시 항목, 이행 상황, 누락 여부를 입력하세요.",
        "focus_fields": ["target_label", "basis_date", "period_start", "period_end"],
        "request_text": "직무고시 관리 현황을 보고드립니다.",
    },
    {
        "category": "안전관리대장관리",
        "code": "SAFE",
        "pdf_heading": "안 전 관 리 대 장",
        "default_title": "안전관리대장 관리",
        "description": "안전관리대장 기록과 유지 상황을 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "안전관리대장 기록 내용과 보완사항을 입력하세요.",
        "focus_fields": ["target_label", "basis_date", "period_start", "period_end"],
        "request_text": "안전관리대장 관리 현황을 보고드립니다.",
    },
    {
        "category": "법정 정기점검",
        "code": "LCHK",
        "pdf_heading": "법 정 정 기 점 검",
        "default_title": "법정 정기점검 결과",
        "description": "법정 주기 점검 결과와 후속 조치를 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "점검 범위, 결과, 후속 조치를 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "basis_date", "period_start", "period_end"],
        "request_text": "법정 정기점검 결과를 보고드립니다.",
    },
    {
        "category": "수질검사",
        "code": "WQ",
        "pdf_heading": "수 질 검 사",
        "default_title": "수질검사 결과",
        "description": "저수조, 급수 계통 등의 수질검사 결과를 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "검사 기관, 검사 결과, 부적합 여부를 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "basis_date", "period_start", "period_end"],
        "request_text": "수질검사 결과를 보고드립니다.",
    },
    {
        "category": "소방정기점검",
        "code": "FIRE",
        "pdf_heading": "소 방 정 기 점 검",
        "default_title": "소방 정기점검 결과",
        "description": "소방시설 정기점검 결과와 보완 조치를 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "소방 점검 결과, 조치 필요 항목, 일정 등을 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "basis_date", "period_start", "period_end"],
        "request_text": "소방 정기점검 결과를 보고드립니다.",
    },
    {
        "category": "기계설비유지관리",
        "code": "MECH",
        "pdf_heading": "기 계 설 비 유 지 관 리",
        "default_title": "기계설비 유지관리 기록",
        "description": "기계설비 유지관리 활동과 조치 이력을 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "유지관리 내용, 조치 결과, 차기 계획을 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "basis_date", "period_start", "period_end"],
        "request_text": "기계설비 유지관리 현황을 보고드립니다.",
    },
    {
        "category": "기계설비성능점검",
        "code": "MPT",
        "pdf_heading": "기 계 설 비 성 능 점 검",
        "default_title": "기계설비 성능점검 결과",
        "description": "기계설비 성능점검 결과와 보완 사항을 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "성능점검 결과와 개선 필요사항을 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "basis_date", "period_start", "period_end"],
        "request_text": "기계설비 성능점검 결과를 보고드립니다.",
    },
    {
        "category": "승강기안전점검",
        "code": "ELV",
        "pdf_heading": "승 강 기 안 전 점 검",
        "default_title": "승강기 안전점검 결과",
        "description": "승강기 안전점검 결과와 부품 교체, 보수 조치를 관리하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "점검 결과, 교체 부품, 지급 사유 등을 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "amount_total", "basis_date"],
        "request_text": "승강기 안전점검 및 후속 조치 결과를 보고드립니다.",
    },
    {
        "category": "안전점검하자보수완료보고",
        "code": "RPR",
        "pdf_heading": "하자보수 완료 보고",
        "default_title": "안전점검 하자보수 완료보고",
        "description": "안전점검 후 하자 보수 완료 결과를 보고하는 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "하자 내용, 보수 결과, 완료 확인 사항을 입력하세요.",
        "focus_fields": ["target_label", "vendor_name", "amount_total", "basis_date"],
        "request_text": "하자보수 완료 결과를 보고드립니다.",
    },
    {
        "category": "기타",
        "code": "ETC",
        "pdf_heading": "행 정 문 서",
        "default_title": "기타 행정 문서",
        "description": "위 분류에 속하지 않는 일반 행정 문서입니다.",
        "amount_policy": "",
        "summary_placeholder": "문서 목적과 주요 내용을 입력하세요.",
        "focus_fields": ["target_label", "basis_date"],
        "request_text": "위 사항을 보고드립니다.",
    },
]

DOCUMENT_CATEGORY_VALUES = tuple(item["category"] for item in DOCUMENT_CATEGORY_PROFILES)
DOCUMENT_CATEGORY_CODES = {item["category"]: item["code"] for item in DOCUMENT_CATEGORY_PROFILES}
DOCUMENT_CATEGORY_PROFILE_MAP = {item["category"]: json.loads(json.dumps(item, ensure_ascii=False)) for item in DOCUMENT_CATEGORY_PROFILES}
LEGACY_DOCUMENT_CATEGORY_ALIASES = {
    "계약": "계약서관리",
    "공문": "기타",
    "보고": "월업무보고(작업 보고서)",
    "예산": "구매요청서(10만원 이하)",
    "입주": "기타",
    "점검": "법정 정기점검",
    "기타": "기타",
}


def document_category_profiles() -> List[Dict[str, Any]]:
    return json.loads(json.dumps(DOCUMENT_CATEGORY_PROFILES, ensure_ascii=False))


def document_common_field_definitions() -> List[Dict[str, Any]]:
    return json.loads(json.dumps(COMMON_DOCUMENT_FIELD_DEFINITIONS, ensure_ascii=False))


def get_document_category_profile(category: Any) -> Dict[str, Any]:
    normalized = normalize_document_category(category, default="기타")
    return json.loads(json.dumps(DOCUMENT_CATEGORY_PROFILE_MAP[normalized], ensure_ascii=False))


def normalize_document_category(category: Any, *, default: str = "기타") -> str:
    raw = str(category or "").strip()
    if not raw:
        return default
    if raw in DOCUMENT_CATEGORY_PROFILE_MAP:
        return raw
    mapped = LEGACY_DOCUMENT_CATEGORY_ALIASES.get(raw)
    if mapped and mapped in DOCUMENT_CATEGORY_PROFILE_MAP:
        return mapped
    return default


def document_category_db_values(category: Any) -> List[str]:
    normalized = normalize_document_category(category, default="기타")
    values = [normalized]
    for legacy, current in LEGACY_DOCUMENT_CATEGORY_ALIASES.items():
        if current == normalized and legacy not in values:
            values.append(legacy)
    return values

