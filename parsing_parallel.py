import os
import json
import tempfile
from uuid import uuid4
from pathlib import Path
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

from pypdf import PdfReader, PdfWriter
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
load_dotenv()

class Parser:
    DEFAULT_BUCKET = os.getenv("DEFAULT_BUCKET", "cesgs-dart")

    def __init__(self):
        # Initialize GCS client and Docling converter once
        self.storage_client = storage.Client()

        pipeline_opts = PdfPipelineOptions()
        pipeline_opts.do_ocr = False
        pipeline_opts.do_table_structure = True
        pipeline_opts.table_structure_options.do_cell_matching = True

        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_opts,
                    backend=DoclingParseV4DocumentBackend
                )
            }
        )

    def _download_pdf(self, url: str) -> Path:
        """Download a PDF from URL and return local Path."""
        resp = requests.get(url)
        resp.raise_for_status()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        tmp.write(resp.content)
        tmp.close()
        return Path(tmp.name)

    def _upload_to_gcs(self, data: list, doc_id: str) -> None:
        """Upload JSON list to GCS under parsed/{doc_id}.json"""
        bucket = self.storage_client.bucket(self.DEFAULT_BUCKET)
        blob = bucket.blob(f"parsed/{doc_id}.json")
        blob.upload_from_string(json.dumps(data, indent=4), content_type="application/json")

    def parse(self, source: str, doc_id: str, testing: bool = False) -> list[dict]:
        """
        Parse PDF by splitting into pages and batch-converting pages.
        Returns list of page dicts and uploads JSON to GCS.
        If testing=True, limit to first 5 pages.
        """
        # 1. Download or use local PDF
        if source.startswith(("http://", "https://")):
            pdf_path = self._download_pdf(source)
        else:
            pdf_path = Path(source)

        # 2. Read all pages
        reader = PdfReader(str(pdf_path))
        total_pages = len(reader.pages)
        limit = min(total_pages, 5) if testing else total_pages

        # 3. Extract each page to a temp file and track mapping
        page_paths = []
        metadata_map = {}
        for i in range(limit):
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            writer = PdfWriter()
            writer.add_page(reader.pages[i])
            writer.write(tmp)
            tmp.close()
            page_paths.append(Path(tmp.name))
            metadata_map[str(tmp.name)] = i + 1

        # 4. Batch convert all page PDFs
        # convert_all expects positional args: list of paths, raises_on_error
        results = self.converter.convert_all(
            page_paths,
            raises_on_error=True
        )

        # 5. Collect parsed pages
        parsed = []
        for res in results:
            page_file = str(res.input.file)
            page_num = metadata_map.get(page_file, None)
            if res.status.name == "FAILURE":
                print(f"Failed parsing page {page_num} of doc {doc_id}")
                continue
            md = res.document.export_to_markdown()
            parsed.append({
                "id": str(uuid4()),
                "page_content": md,
                "metadata": {"page_number": page_num, "doc_id": doc_id}
            })
            print(f"Parsed page {page_num}/{limit} for doc {doc_id}")

        # 6. Upload JSON to GCS
        self._upload_to_gcs(parsed, doc_id)

        # 7. Cleanup temp files
        for p in page_paths:
            try:
                p.unlink()
            except OSError:
                pass
        if source.startswith(("http://", "https://")):
            try:
                pdf_path.unlink()
            except OSError:
                pass

        return parsed

if __name__ == "__main__":
    df = pd.read_excel("documents.xlsx")
    docs = [(row["source"], str(row["doc_id"])) for _, row in df.iterrows()]

    parser = Parser()
    for idx, (source, doc_id) in enumerate(docs, 1):
        print(f"[{idx}/{len(docs)}] Processing doc_id={doc_id}...")
        try:
            parser.parse(source, doc_id, testing=True)
            print("Done")
        except Exception as e:
            print(f"Error processing {doc_id}: {e}")
    print("All documents processed.")
