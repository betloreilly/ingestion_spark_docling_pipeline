"""
Minimal Docling import test for watsonx.data Spark.
Assumes packages were pre-installed via ae.spark.librarysets — no pip install here.

Steps:
  1  SparkSession
  2  Patch asr_pipeline stub (breaks the transformers→accelerate circular import
     that exists on this Spark runtime's system Python)
  3  Import docling.document_converter
  4  Instantiate DocumentConverter (no PDF processed yet)
  5  Write result to S3 using the applicationId so the path is always unique

Note on the circular import:
  docling.document_converter imports asr_pipeline
    → base_pipeline → granite_vision → transformers → accelerate → circular error
  We stub only the asr_pipeline module.  AsrPipeline (audio) is never used for
  PDF work, so the stub is safe.
"""
from pyspark.sql import SparkSession


def main():
    import sys

    def log(msg):
        print(f"[DOCLING-TEST] {msg}", flush=True)

    log("===== DOCLING IMPORT TEST START =====")
    log(f"Python {sys.version}")

    # ── Step 1: SparkSession ──────────────────────────────────────────────────
    spark = SparkSession.builder.appName("docling-libset-test").getOrCreate()
    app_id = spark.sparkContext.applicationId
    log(f"Step 1 PASS  SparkSession  app={app_id}  Spark {spark.version}")

    status = "UNKNOWN"

    try:
        # ── Step 2: patch asr_pipeline stub ──────────────────────────────────
        import types
        _asr_stub = types.ModuleType("docling.pipeline.asr_pipeline")
        _asr_stub.AsrPipeline = type("AsrPipeline", (), {
            "get_scope": classmethod(lambda cls: []),
        })
        sys.modules["docling.pipeline.asr_pipeline"] = _asr_stub
        log("Step 2 PASS  asr_pipeline stub applied")

        # ── Step 3: import docling ────────────────────────────────────────────
        from docling.document_converter import DocumentConverter, PdfFormatOption
        log("Step 3 PASS  docling.document_converter imported")

        # ── Step 4: instantiate converter (no PDF yet) ────────────────────────
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.document_converter import InputFormat

        opts = PdfPipelineOptions()
        opts.do_ocr = False                  # skip OCR model (needs tesseract)
        opts.do_table_structure = True       # table parsing is pure Python
        opts.generate_picture_images = False # skip vision/chart model

        converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=opts)}
        )
        log("Step 4 PASS  DocumentConverter() instantiated")

        status = "SUCCESS"
        log("===== ALL STEPS PASSED =====")

    except Exception as e:
        import traceback
        status = f"FAILED: {type(e).__name__}: {str(e)[:150]}"
        log(f"STEP FAILED: {status}")
        log("--- traceback ---")
        for line in traceback.format_exc().splitlines():
            log(line)
        log("--- end traceback ---")

    # ── Step 5: write result to S3 ────────────────────────────────────────────
    log(f"FINAL STATUS: {status}")
    out_path = f"s3a://boreillyopensearch/logs/doctest_{app_id}"
    try:
        spark.sparkContext.parallelize([status], 1).saveAsTextFile(out_path)
        log(f"Step 5 PASS  result written → {out_path}/part-00000")
    except Exception as e:
        log(f"Step 5 FAIL  S3 write failed: {type(e).__name__}: {e}")

    spark.stop()
    log("===== DONE =====")


if __name__ == "__main__":
    main()
