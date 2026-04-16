import tempfile
from pathlib import Path

from pypdf import PdfReader, PdfWriter

MAX_PAGES = 12


def limit_pdf_pages(pdf_path: Path, max_pages: int = MAX_PAGES) -> Path:
    """
    Se il PDF ha più di max_pages pagine,
    crea una copia temporanea con solo le prime max_pages.
    Ritorna il path del PDF da usare (originale o ridotto).
    """
    reader = PdfReader(str(pdf_path))
    num_pages = len(reader.pages)

    if num_pages <= max_pages:
        return pdf_path

    writer = PdfWriter()
    for index in range(max_pages):
        writer.add_page(reader.pages[index])

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    with open(tmp.name, "wb") as file_obj:
        writer.write(file_obj)

    print(f"[INFO] {pdf_path.name}: {num_pages} pagine -> tagliato a {max_pages}")
    return Path(tmp.name)
