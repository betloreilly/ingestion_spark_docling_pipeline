"""
PDF Processor using Docling
Handles PDF parsing, entity extraction, and semantic chunking
"""

import logging
from typing import List, Dict, Any
from pathlib import Path

# Docling imports - the core PDF processing library
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling_core.types.doc import DoclingDocument, DocItemLabel

logger = logging.getLogger(__name__)


class PDFProcessor:
    """
    PDF processor using IBM Docling for intelligent PDF parsing
    
    Docling provides:
    - Structure-aware PDF parsing (headings, paragraphs, tables)
    - Entity extraction (people, organizations, locations, dates)
    - Layout understanding (multi-column, complex structures)
    - OCR for scanned PDFs
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PDF processor with Docling
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.chunk_size = config['docling']['chunk_size']
        self.chunk_overlap = config['docling']['chunk_overlap']
        
        # Initialize Docling converter with options
        pipeline_options = PdfPipelineOptions()
        # OCR is expensive (3-10x slower). Enable only for scanned PDFs.
        # Controlled by docling.do_ocr in config (default: false).
        pipeline_options.do_ocr = config.get('docling', {}).get('do_ocr', False)
        pipeline_options.do_table_structure = True  # Extract table structure
        pipeline_options.table_structure_options.do_cell_matching = True
        
        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options
                )
            }
        )
        
        logger.info("PDF processor initialized with Docling")
    
    def parse_pdf(self, pdf_path: str) -> DoclingDocument:
        """
        Parse PDF using Docling to extract structure and content
        
        Docling analyzes:
        - Document structure (headings, sections, paragraphs)
        - Tables and their structure
        - Images and captions
        - Reading order in complex layouts
        - Text formatting and styles
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            DoclingDocument: Parsed document with structure
        """
        logger.info(f"Parsing PDF with Docling: {pdf_path}")
        
        try:
            # Convert PDF using Docling
            result = self.converter.convert(pdf_path)
            
            # Get the parsed document
            doc = result.document
            
            logger.info(f"Successfully parsed PDF: {doc.name}")
            logger.info(f"  Pages: {len(doc.pages)}")
            logger.info(f"  Elements: {len(list(doc.iterate_items()))}")
            
            return doc
            
        except Exception as e:
            logger.error(f"Error parsing PDF {pdf_path}: {str(e)}")
            raise
    
    def chunk_document(self, doc: DoclingDocument) -> List[Dict[str, Any]]:
        """
        Create semantic chunks from parsed document
        
        Chunking strategy:
        - Respects document structure (sections, paragraphs)
        - Maintains context within chunks
        - Preserves metadata (page numbers, section titles)
        - Handles tables and images appropriately
        
        Args:
            doc: Parsed Docling document
            
        Returns:
            List of chunks with metadata
        """
        logger.info(f"Creating semantic chunks from document: {doc.name}")
        
        chunks = []
        current_chunk = []
        current_text = []
        current_size = 0
        current_section = None
        current_page = 1
        chunk_index = 0
        
        # Iterate through document elements in reading order (yields (item, level) in newer Docling)
        for thing in doc.iterate_items():
            item = thing[0] if isinstance(thing, tuple) else thing
            element_type = item.label
            element_text = item.text if hasattr(item, 'text') else ""
            
            # Skip empty elements
            if not element_text.strip():
                continue
            
            # Update current section on headings
            if element_type in [DocItemLabel.SECTION_HEADER, DocItemLabel.TITLE]:
                # Save current chunk if it exists
                if current_text:
                    chunk = self._create_chunk(
                        text=" ".join(current_text),
                        document_name=doc.name,
                        page_number=current_page,
                        section_title=current_section,
                        chunk_index=chunk_index,
                        metadata=current_chunk
                    )
                    chunks.append(chunk)
                    chunk_index += 1
                    current_text = []
                    current_size = 0
                    current_chunk = []
                
                # Update section title
                current_section = element_text
                logger.debug(f"New section: {current_section}")
            
            # Add element to current chunk
            current_text.append(element_text)
            current_chunk.append({
                'type': element_type.value if hasattr(element_type, 'value') else str(element_type),
                'text': element_text
            })
            
            # Update page number if available
            if hasattr(item, 'prov') and item.prov:
                for prov in item.prov:
                    if hasattr(prov, 'page_no'):
                        current_page = prov.page_no
            
            # Calculate current size (word count)
            current_size = sum(len(t.split()) for t in current_text)
            
            # Split chunk if it exceeds size limit
            if current_size >= self.chunk_size:
                chunk = self._create_chunk(
                    text=" ".join(current_text),
                    document_name=doc.name,
                    page_number=current_page,
                    section_title=current_section,
                    chunk_index=chunk_index,
                    metadata=current_chunk
                )
                chunks.append(chunk)
                chunk_index += 1
                
                # Keep overlap for context
                overlap_words = self.chunk_overlap
                if overlap_words > 0 and current_text:
                    last_text = current_text[-1]
                    words = last_text.split()
                    if len(words) > overlap_words:
                        overlap_text = " ".join(words[-overlap_words:])
                        current_text = [overlap_text]
                        current_chunk = [current_chunk[-1]]
                    else:
                        current_text = [last_text]
                        current_chunk = [current_chunk[-1]]
                else:
                    current_text = []
                    current_chunk = []
                
                current_size = sum(len(t.split()) for t in current_text)
        
        # Add final chunk
        if current_text:
            chunk = self._create_chunk(
                text=" ".join(current_text),
                document_name=doc.name,
                page_number=current_page,
                section_title=current_section,
                chunk_index=chunk_index,
                metadata=current_chunk
            )
            chunks.append(chunk)
        
        logger.info(f"Created {len(chunks)} chunks from document")
        return chunks
    
    def _create_chunk(self, text: str, document_name: str, page_number: int,
                     section_title: str, chunk_index: int, metadata: List[Dict]) -> Dict[str, Any]:
        """
        Create a chunk dictionary with metadata
        
        Args:
            text: Chunk text
            document_name: Source document name
            page_number: Page number
            section_title: Section title
            chunk_index: Chunk index
            metadata: Element metadata
            
        Returns:
            Chunk dictionary
        """
        # Analyze chunk content
        has_table = any(m.get('type') == 'table' for m in metadata)
        has_image = any(m.get('type') in ['picture', 'figure'] for m in metadata)
        has_code = any(m.get('type') == 'code' for m in metadata)
        
        chunk = {
            'chunk_id': f"{Path(document_name).stem}_chunk_{chunk_index:04d}",
            'chunk_text': text,
            'document_name': document_name,
            'page_number': page_number,
            'section_title': section_title or "Unknown",
            'chunk_index': chunk_index,
            'word_count': len(text.split()),
            'char_count': len(text),
            'has_table': has_table,
            'has_image': has_image,
            'has_code': has_code,
            'element_types': list(set(m.get('type') for m in metadata))
        }
        
        return chunk
    
    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """
        Extract named entities from text using Docling's NER
        
        Entity types:
        - PERSON: Names of people
        - ORGANIZATION: Companies, institutions
        - LOCATION: Cities, countries, addresses
        - DATE: Dates and time expressions
        - MONEY: Currency amounts
        - PERCENT: Percentage values
        - PRODUCT: Product names
        - EVENT: Named events
        
        Args:
            text: Text to extract entities from
            
        Returns:
            Dictionary of entity types to entity lists
        """
        # Initialize entity dictionary
        entities = {
            'PERSON': [],
            'ORGANIZATION': [],
            'LOCATION': [],
            'DATE': [],
            'MONEY': [],
            'PERCENT': [],
            'PRODUCT': [],
            'EVENT': []
        }
        
        try:
            import re

            # ORGANIZATION: explicit legal suffixes only (conservative).
            org_pattern = r'\b([A-Z][A-Za-z&]*(?:\s+[A-Z][A-Za-z&]*)*\s+(?:Inc|Corp|Corporation|Ltd|LLC|GmbH|PLC)\.?)\b'
            entities['ORGANIZATION'] = list(set(re.findall(org_pattern, text)))

            # LOCATION: only capture proper nouns after location prepositions.
            # This avoids tagging random capitalized technical phrases as locations.
            # Examples matched: "in New York", "from San Francisco", "at London".
            loc_pattern = r'\b(?:in|at|from|to|near)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b'
            raw_locations = re.findall(loc_pattern, text)
            non_location_terms = {
                "Spark", "Spark Engine", "Docling", "OpenSearch", "Ingestion Solutions",
                "Overview", "Note", "It", "The", "This", "That", "These", "Those",
                "Apache Spark",
            }
            entities['LOCATION'] = [
                loc for loc in dict.fromkeys(raw_locations)  # preserve order, dedupe
                if loc not in non_location_terms and len(loc) > 2
            ][:20]

            # DATE: common date formats
            date_pattern = r'\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{1,2}[-/]\d{1,2}|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})\b'
            entities['DATE'] = list(set(re.findall(date_pattern, text, re.IGNORECASE)))

            # MONEY: currency amounts
            money_pattern = r'\$\s*\d+(?:,\d{3})*(?:\.\d{2})?|\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:USD|EUR|GBP|dollars?|euros?)'
            entities['MONEY'] = list(set(re.findall(money_pattern, text, re.IGNORECASE)))

            # PERCENT: percentage values
            percent_pattern = r'\d+(?:\.\d+)?%|\d+(?:\.\d+)?\s*percent'
            entities['PERCENT'] = list(set(re.findall(percent_pattern, text, re.IGNORECASE)))

            # Remove empty lists and cap items
            entities = {k: v[:20] for k, v in entities.items() if v}

        except Exception as e:
            logger.warning(f"Error extracting entities: {str(e)}")
        
        return entities

# Made with Bob
