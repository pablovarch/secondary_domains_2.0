import argparse
import os
import pathlib
import re
from typing import List

import openai
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

from dependencies import  log
from settings import db_connect
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class ssl_analyzer :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='graymarket.log')

    def main(self):



        df = self.process_html(html)



# --------------------------- UTILIDADES --------------------------- #

    def extract_visible_text(self, html: str) -> str:
        """Elimina <script>, <style> y devuelve texto plano compactado."""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
        return re.sub(r"\s+", " ", text).strip()


    def llm_classify(self, text: str) -> str:
        """Envía un prompt al LLM y valida que devuelva SOLO la etiqueta prevista."""
        # --------------------------- CONFIGURACIÓN --------------------------- #
        openai.api_key = os.getenv("OPENAI_API_KEY")  # clave en variable de entorno
        MODEL = "gpt-4o-mini"  # 4.1‑mini ≈ gpt‑4o‑mini
        TEMPERATURE = 0

        ALLOWED_LABELS = {
            "Adult Content",
            "Gambling & Betting",
            "Cryptocurrency Speculation",
            "Supplement / Nutra",
            "undeterminated",
        }

        prompt = (
            "You are a strict classification engine for compliance screening.\n"
            "Task: Read the web‑page excerpt (it may be in ANY language) and output ONE label, EXACTLY as written below, or 'undeterminated' if none apply.\n\n"
            "• Adult Content ‑ Pornography, escort services, explicit sexual material, or products aimed at sexual performance/enhancement.\n"
            "• Gambling & Betting ‑ Websites facilitating or promoting gambling, including casinos, sports betting, lotteries, fantasy sports, or any wagering services.\n"
            "• Cryptocurrency Speculation ‑ Content primarily focused on high‑risk or unregulated crypto tokens, NFT promotions, get‑rich‑quick schemes, pump‑and‑dump communities, or speculative trading signals.\n"
            "• Supplement / Nutra ‑ Sites marketing dietary or nutritional supplements, vitamins, weight‑loss pills, muscle enhancers, anti‑aging or sexual health supplements.\n\n"
            "If none of the above fit, respond with the single word: undeterminated.\n"
            "‼️ VERY IMPORTANT: Respond with the label ONLY. No explanations or extra text.\n\n"
            # "Excerpt (truncated if lengthy):\n"""\n" + text[:4500] + "\n""""
                )
        messages = [
            {"role": "system", "content": "You are a text‑classification engine."},
            {"role": "user", "content": prompt},
        ]
        response = openai.ChatCompletion.create(
            model=MODEL,
            temperature=TEMPERATURE,
            messages=messages,
        )
        raw = response.choices[0].message.content.strip()
        label = raw.splitlines()[0]             # descarta posibles líneas extra
        label = re.sub(r"[^\w &/]", "", label).strip()
        if label not in ALLOWED_LABELS:
            return "undeterminated"
        return label

# --------------------------- PIPELINE PRINCIPAL --------------------------- #

    def process_html(self, html) :
        """Procesa una lista de HTMLs y devuelve un DataFrame con las columnas requeridas."""

        visible_text = self.extract_visible_text(html)
        graymarket_label= self.llm_classify(visible_text)

        return graymarket_label

