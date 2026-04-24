"""Defaults de keywords e janela de tempo."""
from __future__ import annotations

DEFAULT_KEYWORDS: list[str] = [
    "petróleo", "petroleo",
    "Petrobras",
    "Vibra",
    "Brava",
    "Ultrapar",
    "Ipiranga",
    "PetroReconcavo", "PetroRecôncavo",
    "oil",
    "gasolina",
    "gás", "gas",
    "diesel",
    "combustível", "combustivel",
    "combustíveis", "combustiveis",
    "OceanPact",
    "Cosan",
    "Raízen", "Raizen",
    "Braskem",
    "Compass",
    "PRIO",
]

DEFAULT_WINDOW_HOURS: int = 24

WINDOW_PRESETS: list[int] = [1, 3, 6, 12, 24, 48, 72, 168]
