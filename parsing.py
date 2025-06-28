import os
import json
import requests
import tempfile
from uuid import uuid4
from pypdf import PdfReader, PdfWriter
from pypdf.errors import PdfReadError
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from google.cloud import storage
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env
load_dotenv()

# Required env: GOOGLE_APPLICATION_CREDENTIALS

class Parser:
    DEFAULT_BUCKET = os.getenv("DEFAULT_BUCKET", "cesgs-dart")
    OCR_LANG = os.getenv("OCR_LANGUAGES", "es").split(",")
    OCR_THREADS = int(os.getenv("OCR_THREADS", "4"))

    @staticmethod
    def _download_pdf(url: str) -> str:
        """Download a PDF from URL and return local path."""
        response = requests.get(url)
        response.raise_for_status()
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        tmp_file.write(response.content)
        tmp_file.close()
        return tmp_file.name

    @staticmethod
    def get_pdf_page_count(pdf_path: str) -> int | None:
        """Return number of pages in PDF or None on error."""
        if not os.path.exists(pdf_path):
            return None
        try:
            reader = PdfReader(pdf_path)
            return len(reader.pages)
        except PdfReadError:
            return None
        except Exception:
            return None

    @staticmethod
    def extract_single_page(input_path: str, page_number: int, output_path: str) -> None:
        """Extract one page to new PDF file."""
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        reader = PdfReader(input_path)
        total = len(reader.pages)
        if not (1 <= page_number <= total):
            raise IndexError(f"Page number {page_number} out of range (1-{total})")
        writer = PdfWriter()
        writer.add_page(reader.pages[page_number - 1])
        with open(output_path, "wb") as f:
            writer.write(f)

    @staticmethod
    def parse_single_page(input_doc_path: str) -> str:
        """Run OCR and conversion on single-page PDF and return Markdown text."""
        pipeline_opts = PdfPipelineOptions()
        pipeline_opts.do_ocr = True
        pipeline_opts.do_table_structure = True
        pipeline_opts.table_structure_options.do_cell_matching = True
        pipeline_opts.ocr_options.lang = Parser.OCR_LANG
        pipeline_opts.accelerator_options = AcceleratorOptions(
            num_threads=Parser.OCR_THREADS,
            device=AcceleratorDevice.AUTO
        )
        converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts)}
        )
        result = converter.convert(input_doc_path)
        return result.document.export_to_markdown()

    @staticmethod
    def _upload_to_gcs(data: dict | list, bucket_name: str, path: str) -> None:
        """Upload JSON-serializable object to GCS at specified path."""
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(path)
        blob.upload_from_string(json.dumps(data), content_type="application/json")

    @staticmethod
    def parse(source: str, doc_id: str, bucket_name: str = None, testing: bool = False) -> list[dict]:
        """Parse PDF (path or URL), upload JSON to GCS, return list of page dicts. If testing=True, limit to first 5 pages."""
        if bucket_name is None:
            bucket_name = Parser.DEFAULT_BUCKET

        local_pdf = Parser._download_pdf(source) if source.startswith(('http://', 'https://')) else source
        num_pages = Parser.get_pdf_page_count(local_pdf)
        if num_pages is None:
            raise ValueError(f"Could not read PDF at {local_pdf}")

        limit = min(num_pages, 5) if testing else num_pages
        parsed = []
        for page_number in range(1, limit + 1):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as page_file:
                page_path = page_file.name
            Parser.extract_single_page(local_pdf, page_number, page_path)
            try:
                text = Parser.parse_single_page(page_path)
            finally:
                os.remove(page_path)
            parsed.append({"id": str(uuid4()), "page_content": text, "metadata": {"page_number": page_number, "doc_id": doc_id}})

        gcs_path = f"parsed/{doc_id}.json"
        Parser._upload_to_gcs(parsed, bucket_name, gcs_path)

        if source.startswith(('http://', 'https://')) and os.path.exists(local_pdf):
            os.remove(local_pdf)
        return parsed


if __name__ == "__main__":
    df = pd.read_excel("documents.xlsx")
    total = len(df)
    print(f"Starting parsing {total} documents...")
    for idx, row in df.iterrows():
        source = row["source"]
        doc_id = str(row["doc_id"])
        testing = True
        print(f"[{idx+1}/{total}] Processing doc_id={doc_id}...", end=" ")
        try:
            Parser.parse(source, doc_id, testing=testing)
            print("Done")
        except Exception as e:
            print(f"Error: {e}")
    print("All documents processed.")