from __future__ import annotations

import subprocess
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config.models import BrowserDebugConfig

LOGGER = get_logger(__name__)
CHALLENGE_TITLE_TOKENS = ("just a moment", "chờ một chút")
CHALLENGE_BODY_TOKENS = (
    "verify you are human",
    "performing security verification",
    "enable javascript and cookies",
    "xác minh bạn không phải là bot",
    "网络错误,请点击刷新按钮重试",
)
RATE_LIMIT_TITLE_TOKENS = (
    "error 1015",
    "access denied",
    "出错了",
)
RATE_LIMIT_BODY_TOKENS = (
    "you are being rate limited",
    "banned you temporarily",
    "temporarily from accessing this website",
    "访问太频繁了",
    "请30秒过后刷新重试",
)


class ChallengePolicy:
    def __init__(self, browser_config: BrowserDebugConfig) -> None:
        self.browser_config = browser_config

    def detect(self, html: str, title: str = "") -> bool:
        return self.classify(html, title) != ""

    def classify(self, html: str, title: str = "") -> str:
        lower_title = title.lower()
        lower_html = html.lower()
        if any(token in lower_title for token in RATE_LIMIT_TITLE_TOKENS):
            return "rate_limited"
        if any(token in lower_html for token in RATE_LIMIT_BODY_TOKENS):
            return "rate_limited"
        if any(token in lower_title for token in CHALLENGE_TITLE_TOKENS):
            return "challenge"
        if any(token in lower_html for token in CHALLENGE_BODY_TOKENS):
            return "challenge"
        return ""

    def debug_image_dir(self) -> Path:
        path = Path(self.browser_config.debug_image_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def launch_debug_browser(self, url: str) -> None:
        executable = self.browser_config.executable_path or "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        args = [
            executable,
            f"--remote-debugging-port={self.browser_config.remote_debugging_port}",
            "--no-first-run",
            "--disable-blink-features=AutomationControlled",
        ]
        if self.browser_config.user_data_dir:
            args.append(f"--user-data-dir={self.browser_config.user_data_dir}")
        if self.browser_config.profile_directory:
            args.append(f"--profile-directory={self.browser_config.profile_directory}")
        args.append(url)
        LOGGER.info("Launching debug browser: %s", " ".join(args))
        subprocess.Popen(args)

    def should_try_browser_fallback(self) -> bool:
        return self.browser_config.mode in {"auto", "debug-attach", "debug-launch"}
