# Sample PDF Files

This directory should contain sample PDF files for testing the ingestion pipeline.

## Adding Sample PDFs

Place your PDF files in this directory. The pipeline will process all `.pdf` files found here.

## Recommended Test Files

For comprehensive testing, include PDFs with:

1. **Simple text documents** - Basic text-only PDFs
2. **Multi-page documents** - Documents with 10+ pages
3. **Documents with tables** - To test table extraction
4. **Documents with images** - To test image handling (if OCR is enabled)
5. **Technical documents** - With code snippets, formulas, or technical content
6. **Mixed content** - Documents with various formatting styles

## Sample Content Suggestions

If you don't have PDFs readily available, you can:

1. Convert markdown files to PDF using tools like pandoc
2. Use publicly available technical documentation
3. Create simple PDFs from text editors
4. Use sample documents from open datasets

## Example Files to Create

Create simple test PDFs with content like:

### test_document_1.pdf
```
Title: Introduction to Machine Learning

Machine learning is a subset of artificial intelligence that focuses on 
building systems that can learn from data. These systems improve their 
performance over time without being explicitly programmed.

Key concepts include:
- Supervised learning
- Unsupervised learning
- Reinforcement learning
```

### test_document_2.pdf
```
Title: Cloud Computing Overview

Cloud computing provides on-demand access to computing resources over the 
internet. Major cloud providers include AWS, Azure, and Google Cloud.

Benefits:
- Scalability
- Cost efficiency
- Global reach
- High availability
```

### test_document_3.pdf
```
Title: Data Processing Pipelines

Data pipelines are essential for modern data-driven applications. They 
automate the flow of data from source to destination, applying 
transformations along the way.

Common stages:
1. Extract - Read data from sources
2. Transform - Clean and process data
3. Load - Write data to target systems
```

## Notes

- Minimum recommended: 3-5 sample PDFs
- Total size: Keep under 50MB for quick testing
- Ensure PDFs are not password-protected
- Use PDFs with searchable text (not scanned images unless OCR is enabled)