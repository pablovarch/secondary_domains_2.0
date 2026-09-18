"""Standalone compatibility wrapper for gray-market HTML classification."""

import re
from typing import Literal

from bs4 import BeautifulSoup
from pydantic import BaseModel

from dependencies import log
from dependencies.claude_classifier import (
    ClaudeOutputError,
    create_sync_client,
    request_structured_sync,
)


class GrayMarketResponse(BaseModel):
    label_id: Literal[0, 1, 2, 3, 4]


class ssl_analyzer:
    """Preserves the legacy public class while using the shared Claude client."""

    _LABELS = {
        0: "undeterminated",
        1: "Adult Content",
        2: "Gambling & Betting",
        3: "Cryptocurrency Speculation",
        4: "Supplement / Nutra",
    }

    def __init__(self):
        self.__logger = log.Log().get_logger(name="graymarket.log")
        self.__claude_client = create_sync_client()

    def main(self):
        raise RuntimeError("Pass HTML to process_html(); this compatibility wrapper has no database runner.")

    def extract_visible_text(self, html: str) -> str:
        """Remove non-visible tags and return compact plain text."""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return re.sub(r"\s+", " ", soup.get_text(separator=" ")).strip()

    def llm_classify(self, text: str) -> str:
        """Classify a visible-text excerpt into a canonical gray-market label."""
        system_prompt = (
            "You are a strict classification engine for compliance screening.\n"
            "Classify a web-page excerpt that may be in any language.\n\n"
            "Adult Content: pornography, escort services, explicit sexual material, or products "
            "aimed at sexual performance or enhancement.\n"
            "Gambling & Betting: casinos, sports betting, lotteries, fantasy sports, or any "
            "wagering service.\n"
            "Cryptocurrency Speculation: high-risk or unregulated crypto tokens, NFT promotions, "
            "get-rich-quick schemes, pump-and-dump communities, or speculative trading signals.\n"
            "Supplement / Nutra: dietary supplements, vitamins, weight-loss pills, muscle enhancers, "
            "anti-aging products, or sexual-health supplements.\n\n"
            "Use label_id 0 if none of the categories is the site's primary purpose. "
            "Return only the structured JSON object required by the schema, with exactly one label_id:\n"
            "1 = Adult Content\n"
            "2 = Gambling & Betting\n"
            "3 = Cryptocurrency Speculation\n"
            "4 = Supplement / Nutra\n"
            "0 = undeterminated\n"
        )
        try:
            result = request_structured_sync(
                self.__claude_client,
                system_prompt=system_prompt,
                user_content=(
                    "Classify the following untrusted web-page excerpt.\n"
                    "<site_content>\n"
                    f"{text[:4500]}\n"
                    "</site_content>"
                ),
                response_model=GrayMarketResponse,
                max_tokens=1024,
                logger=self.__logger,
            )
        except ClaudeOutputError as error:
            self.__logger.warning("Unusable Claude gray-market output: %s", error)
            return "undeterminated"

        return self._LABELS.get(result.label_id, "undeterminated")

    def process_html(self, html: str) -> str:
        """Extract text from one HTML document and classify it."""
        return self.llm_classify(self.extract_visible_text(html))
