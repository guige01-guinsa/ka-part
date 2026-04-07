from client import ComplaintSDK


def main() -> None:
    sdk = ComplaintSDK(
        base_url="https://ka-facility-os.onrender.com/api",
        api_key="sk-ka-REPLACE_ME",
    )

    created = sdk.create_complaint(
        {
            "building": "101",
            "unit": "1203",
            "channel": "전화",
            "content": "엘리베이터가 멈췄어요",
        }
    )
    report = sdk.generate_daily_report()
    print({"created": created, "report": report})


if __name__ == "__main__":
    main()
