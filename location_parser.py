import re
from typing import Dict

UF_CODES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}


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
