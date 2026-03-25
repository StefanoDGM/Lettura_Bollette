import tempfile
from pathlib import Path
from pypdf import PdfReader, PdfWriter
from typing import Optional

MAX_PAGES = 8

def limit_pdf_pages(pdf_path: Path, max_pages: int = MAX_PAGES) -> Path:
    """
    Se il PDF ha più di max_pages pagine,
    crea una copia temporanea con solo le prime max_pages.
    Ritorna il path del PDF da usare (originale o ridotto).
    """
    reader = PdfReader(str(pdf_path))
    num_pages = len(reader.pages)

    if num_pages <= max_pages:
        return pdf_path  # usa originale

    writer = PdfWriter()
    for i in range(max_pages):
        writer.add_page(reader.pages[i])

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    with open(tmp.name, "wb") as f:
        writer.write(f)

    print(f"[INFO] {pdf_path.name}: {num_pages} pagine → tagliato a {max_pages}")
    return Path(tmp.name)