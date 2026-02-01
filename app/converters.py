import os
import subprocess
from pathlib import Path


def convert_with_soffice(input_path: str, output_format: str, out_dir: str) -> str:
    """
    Uses LibreOffice (soffice) to convert documents.
    Returns the converted file path if successful.
    """
    soffice = os.getenv("SOFFICE_PATH", "soffice")
    out_dir_path = Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    cmd = [
        soffice,
        "--headless",
        "--convert-to",
        output_format,
        "--outdir",
        str(out_dir_path),
        str(Path(input_path)),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout or "soffice conversion failed")

    out_name = Path(input_path).with_suffix(f".{output_format}").name
    out_path = out_dir_path / out_name
    if not out_path.exists():
        raise RuntimeError("converted file not found")
    return str(out_path)
