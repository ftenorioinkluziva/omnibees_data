import re
from typing import Dict

UF_CODES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}

# CEP prefix ranges (5 digits) → UF mapping based on Correios postal code system
_CEP_RANGES: list[tuple[int, int, str]] = [
    (1000,  19999, "SP"),
    (20000, 28999, "RJ"),
    (29000, 29999, "ES"),
    (30000, 39999, "MG"),
    (40000, 48999, "BA"),
    (49000, 49999, "SE"),
    (50000, 56999, "PE"),
    (57000, 57999, "AL"),
    (58000, 58999, "PB"),
    (59000, 59999, "RN"),
    (60000, 63999, "CE"),
    (64000, 64999, "PI"),
    (65000, 65999, "MA"),
    (66000, 68899, "PA"),
    (68900, 68999, "AP"),
    (69000, 69299, "AM"),
    (69300, 69399, "RR"),
    (69400, 69899, "AM"),
    (69900, 69999, "AC"),
    (70000, 72799, "DF"),
    (72800, 76799, "GO"),
    (76800, 76999, "RO"),
    (77000, 77999, "TO"),
    (78000, 78999, "MT"),
    (79000, 79999, "MS"),
    (80000, 87999, "PR"),
    (88000, 89999, "SC"),
    (90000, 99999, "RS"),
]


def zip_to_state(zip_code: str) -> str:
    digits = re.sub(r"\D", "", zip_code or "")
    if len(digits) != 8:
        return ""
    prefix = int(digits[:5])
    for lo, hi, uf in _CEP_RANGES:
        if lo <= prefix <= hi:
            return uf
    return ""


def _clean_text(text: str) -> str:
    normalized = " ".join((text or "").split())
    return normalized.replace("|", ",")


def _normalize_zip(zip_code: str) -> str:
    digits = re.sub(r"\D", "", zip_code or "")
    if len(digits) == 8:
        return f"{digits[:5]}-{digits[5:]}"
    return ""


def _clean_segment(value: str) -> str:
    cleaned = value or ""
    cleaned = re.sub(r"\b\d{5}-?\d{3}\b", "", cleaned)
    cleaned = re.sub(r"\b(Brasil|Brazil)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(Cidade|City|Estado|State|Endereco|Endereço|Address)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(" + "|".join(sorted(UF_CODES)) + r")\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -/,")
    return cleaned


def parse_location_text(raw_text: str) -> Dict[str, str]:
    text = _clean_text(raw_text)
    if not text:
        return {"address": "", "city": "", "state": "", "zip_code": "", "country": ""}

    segments = [segment.strip(" -/") for segment in re.split(r",", text) if segment.strip()]

    zip_match = re.search(r"\b\d{5}-?\d{3}\b", text)
    zip_code = _normalize_zip(zip_match.group(0)) if zip_match else ""

    state_match = re.search(r"\b(" + "|".join(sorted(UF_CODES)) + r")\b", text, flags=re.IGNORECASE)
    state = state_match.group(1).upper() if state_match else ""

    country = "Brasil" if re.search(r"\b(Brasil|Brazil)\b", text, flags=re.IGNORECASE) else ""

    address = _clean_segment(segments[0]) if segments else ""
    city = ""

    if state:
        for index, segment in enumerate(segments):
            if re.search(rf"\b{state}\b", segment, flags=re.IGNORECASE):
                candidate = _clean_segment(segment)
                if candidate:
                    city = candidate
                elif index > 0:
                    city = _clean_segment(segments[index - 1])
                break

    if not city and len(segments) > 1:
        city = _clean_segment(segments[1])

    if not city:
        city = _clean_segment(text)

    if len(city) > 80:
        city = city[:80].strip()

    return {
        "address": address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "country": country,
    }
