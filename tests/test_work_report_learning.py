from __future__ import annotations

from app.work_report_learning import build_feedback_few_shot_examples, build_feedback_learning_dataset


def test_build_feedback_few_shot_examples_keeps_high_signal_rows() -> None:
    rows = [
        {
            "feedback_type": "change_stage",
            "filename": "ignore-stage.jpg",
            "to_item_title": "104동 센서등 교체",
            "candidate_items_json": "[]",
        },
        {
            "feedback_type": "reassign_item",
            "filename": "KakaoTalk_20260702_091100.jpg",
            "from_item_title": "101동 집하장 센서등 2개교체",
            "to_item_title": "104동 집하장 센서등 1개교체",
            "review_reason": "점수 차가 작습니다",
            "candidate_items_json": (
                '[{"item_index":1,"title":"101동 집하장 센서등 2개교체","score":13},'
                '{"item_index":2,"title":"104동 집하장 센서등 1개교체","score":13}]'
            ),
        },
        {
            "feedback_type": "mark_unmatched",
            "filename": "KakaoTalk_20260702_092000.jpg",
            "review_reason": "작업 사진이 아니라 증빙 캡처입니다",
            "candidate_items_json": '[{"item_index":3,"title":"지하주차장 출입문 보수","score":8}]',
        },
    ]

    examples = build_feedback_few_shot_examples(rows, limit=4)

    assert len(examples) == 2
    assert examples[0]["feedback_type"] == "reassign_item"
    assert examples[0]["decision_label"] == "사람이 다른 작업으로 재배정"
    assert examples[0]["candidate_items"][1]["title"] == "104동 집하장 센서등 1개교체"
    assert examples[1]["feedback_type"] == "mark_unmatched"
    assert examples[1]["decision_label"] == "미매칭 유지"


def test_build_feedback_learning_dataset_exports_feedback_targets() -> None:
    rows = [
        {
            "tenant_id": "ys_thesharp",
            "job_id": "job-1",
            "feedback_type": "confirm_current",
            "filename": "KakaoTalk_20260702_091100.jpg",
            "from_item_index": 2,
            "from_item_title": "104동 집하장 센서등 1개교체",
            "to_item_index": 2,
            "to_item_title": "104동 집하장 센서등 1개교체",
            "candidate_items_json": '[{"item_index":2,"title":"104동 집하장 센서등 1개교체","score":14}]',
        },
        {
            "tenant_id": "ys_thesharp",
            "job_id": "job-1",
            "feedback_type": "mark_unmatched",
            "filename": "capture.jpg",
            "candidate_items_json": '[{"item_index":3,"title":"지하주차장 출입문 보수","score":8}]',
            "to_stage": "after",
        },
    ]

    dataset = build_feedback_learning_dataset(rows)

    assert len(dataset) == 2
    assert dataset[0]["tenant_id"] == "ys_thesharp"
    assert dataset[0]["target"]["decision"] == "keep_current_item"
    assert dataset[0]["input"]["candidate_items"][0]["title"] == "104동 집하장 센서등 1개교체"
    assert dataset[1]["target"]["decision"] == "leave_unmatched"
    assert dataset[1]["target"]["item_index"] == 0


def test_openai_match_chunks_includes_tenant_few_shot_examples(monkeypatch) -> None:
    import app.work_report_service as service

    class DummyClient:
        def with_options(self, **kwargs):
            return self

    captured = {}

    monkeypatch.setattr(service, "_openai_client", lambda **kwargs: (DummyClient(), "gpt-5.4"))

    def fake_openai_json_response(**kwargs):
        captured["content"] = list(kwargs.get("content") or [])
        return {
            "cluster_matches": [{"cluster_index": 1, "item_index": 1, "confidence": "high"}],
            "unmatched_cluster_indexes": [],
            "analysis_notice": "",
        }

    monkeypatch.setattr(service, "_openai_json_response", fake_openai_json_response)

    result = service._openai_match_image_chunks(
        text="\n".join(
            [
                "2026년 7월 2일 수요일",
                "[관리실] [오전 9:10] 104동 집하장 센서등 1개교체",
            ]
        ),
        image_inputs=[
            {
                "filename": "KakaoTalk_20260702_091100.jpg",
                "bytes": b"fake-image",
                "content_type": "image/jpeg",
            }
        ],
        items=[
            {
                "index": 1,
                "title": "104동 집하장 센서등 1개교체",
                "summary": "104동 집하장 센서등 1개교체",
                "location_name": "104동 집하장",
                "work_date": "2026-07-02",
                "work_date_label": "7월 2일",
                "_minute_of_day": (9 * 60) + 10,
            }
        ],
        feedback_profile={
            "tenant_id": "ys_thesharp",
            "rows_used": 1,
            "few_shot_examples": [
                {
                    "feedback_type": "reassign_item",
                    "decision_label": "사람이 다른 작업으로 재배정",
                    "filename": "KakaoTalk_20260630_091100.jpg",
                    "from_item_title": "101동 집하장 센서등 2개교체",
                    "to_item_title": "104동 집하장 센서등 1개교체",
                    "candidate_items": [
                        {"item_index": 1, "title": "101동 집하장 센서등 2개교체", "score": 13},
                        {"item_index": 2, "title": "104동 집하장 센서등 1개교체", "score": 13},
                    ],
                    "review_reason": "점수 차가 작습니다",
                }
            ],
        },
    )

    joined = "\n".join(str(row.get("text") or "") for row in captured["content"] if row.get("type") == "input_text")
    assert "최근 사람 검토 예시" in joined
    assert "사람선택 104동 집하장 센서등 1개교체" in joined
    assert result["analysis_diagnostics"]["few_shot_example_count"] == 1
