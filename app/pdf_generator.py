import os
import subprocess
import tempfile
import shutil

CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

def generate_pdf_from_url(url: str, output_path: str, timeout_sec: int = 120) -> bool:
    try:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        abs_output = os.path.abspath(output_path)
        user_data_dir = tempfile.mkdtemp(prefix="chrome_pdf_")

        try:
            cmd = [
                CHROME,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--no-first-run",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-sync",
                "--hide-scrollbars",
                "--mute-audio",
                "--disable-features=TranslateUI",
                "--run-all-compositor-stages-before-draw",
                f"--user-data-dir={user_data_dir}",
                "--virtual-time-budget=15000",
                "--window-size=1280,10000",
                f"--print-to-pdf={abs_output}",
                "--print-to-pdf-no-header",
                url,
            ]

            print(f"[PDF] Printing: {url}")
            result = subprocess.run(
                cmd,
                timeout=timeout_sec,
                capture_output=True,
                text=True,
            )
            print(f"[PDF] Chrome exit={result.returncode}")

        finally:
            shutil.rmtree(user_data_dir, ignore_errors=True)

        size = os.path.getsize(abs_output) if os.path.exists(abs_output) else 0
        if size > 500:
            print(f"[PDF] OK ({size:,} bytes): {abs_output}")
            return True
        else:
            print(f"[PDF] Failed - size={size} bytes")
            return False

    except subprocess.TimeoutExpired:
        print(f"[PDF] Timeout after {timeout_sec}s for {url}")
        return False
    except Exception as e:
        import traceback
        print(f"[PDF] Error for {url}: {e}")
        traceback.print_exc()
        return False