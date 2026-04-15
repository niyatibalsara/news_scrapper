import os
import re
import traceback
from html import unescape
from xml.sax.saxutils import escape

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.colors import HexColor
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    HRFlowable,
)


def clean_text(text):
    if text is None:
        return ""

    text = str(text)
    text = unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\r", "\n")
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)

    return text.strip()


def format_keywords(keywords):
    if not keywords:
        return []

    if isinstance(keywords, list):
        return [clean_text(k) for k in keywords if clean_text(k)]

    raw = clean_text(keywords)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts if parts else [raw]


def safe_para(text):
    return escape(text).replace("\n", "<br/>")


def generate_pdf_from_article(article_data, output_path):
    try:
        # Validate article_data structure
        if not isinstance(article_data, dict):
            print(f"[PDF ERROR] article_data is not a dict: {type(article_data)}")
            return False
        
        print(f"[PDF] Generating PDF: {output_path}")
        print(f"[PDF] Article keys: {list(article_data.keys())}")
        
        # Create output directory
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            print(f"[PDF] Output directory ready: {output_dir}")

        title = clean_text(article_data.get("news_headline", "Untitled Article"))
        keywords = format_keywords(article_data.get("keywords", ""))
        article_text = clean_text(article_data.get("news_text", ""))
        news_date = clean_text(article_data.get("news_date", ""))
        news_type = clean_text(article_data.get("news_type", ""))
        news_url = clean_text(article_data.get("news_url", ""))
        
        print(f"[PDF] Title: {title[:50]}...")
        print(f"[PDF] Text length: {len(article_text)} chars")
        print(f"[PDF] Keywords: {len(keywords)} items")

        if not article_text:
            article_text = "No article content available."

        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=40,
            leftMargin=40,
            topMargin=40,
            bottomMargin=40,
        )

        styles = getSampleStyleSheet()

        title_style = ParagraphStyle(
            name="CustomTitle",
            parent=styles["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=24,
            textColor=HexColor("#1f2937"),
            alignment=TA_LEFT,
            spaceAfter=12,
        )

        label_style = ParagraphStyle(
            name="LabelStyle",
            parent=styles["Normal"],
            fontName="Helvetica-Bold",
            fontSize=10,
            leading=14,
            textColor=HexColor("#111827"),
        )

        value_style = ParagraphStyle(
            name="ValueStyle",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=HexColor("#374151"),
        )

        body_style = ParagraphStyle(
            name="BodyStyle",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=11,
            leading=17,
            textColor=HexColor("#111827"),
            alignment=TA_LEFT,
        )

        url_style = ParagraphStyle(
            name="UrlStyle",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=9,
            leading=13,
            textColor=HexColor("#2563eb"),
        )

        story = []

        story.append(Paragraph(safe_para(title), title_style))
        story.append(Spacer(1, 8))

        if news_type:
            story.append(Paragraph(f"<b>Category:</b> {safe_para(news_type)}", value_style))
            story.append(Spacer(1, 4))

        if news_date:
            story.append(Paragraph(f"<b>Date:</b> {safe_para(news_date)}", value_style))
            story.append(Spacer(1, 4))

        if keywords:
            story.append(Paragraph("<b>Keywords:</b>", label_style))
            story.append(Spacer(1, 4))
            for kw in keywords:
                story.append(Paragraph(f"• {safe_para(kw)}", body_style))
                story.append(Spacer(1, 2))
        else:
            story.append(Paragraph("<b>Keywords:</b> N/A", value_style))

        story.append(Spacer(1, 10))
        story.append(HRFlowable(width="100%", thickness=1, color=HexColor("#d1d5db")))
        story.append(Spacer(1, 12))

        story.append(Paragraph("Article", label_style))
        story.append(Spacer(1, 8))

        paragraphs = [p.strip() for p in article_text.split("\n\n") if p.strip()]
        if not paragraphs:
            paragraphs = [article_text]

        for para in paragraphs:
            story.append(Paragraph(safe_para(para), body_style))
            story.append(Spacer(1, 10))

        if news_url:
            story.append(Spacer(1, 12))
            story.append(HRFlowable(width="100%", thickness=1, color=HexColor("#d1d5db")))
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Original URL:</b>", label_style))
            story.append(Spacer(1, 4))
            story.append(Paragraph(safe_para(news_url), url_style))

        doc.build(story)
        
        # Verify file was created
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f"[PDF] ✓ Generated successfully: {output_path} ({file_size} bytes)")
            if file_size > 500:
                return True
            else:
                print(f"[PDF ERROR] File too small ({file_size} bytes, need >500)")
                return False
        else:
            print(f"[PDF ERROR] File not created at {output_path}")
            return False

    except TypeError as e:
        print(f"[PDF ERROR] Type error (likely ReportLab issue): {e}")
        print(f"[PDF ERROR] article_data type: {type(article_data)}")
        if isinstance(article_data, dict):
            print(f"[PDF ERROR] article_data keys: {list(article_data.keys())}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"[PDF ERROR] {type(e).__name__}: {e}")
        print(f"[PDF ERROR] output_path: {output_path}")
        if isinstance(article_data, dict):
            print(f"[PDF ERROR] article data keys: {list(article_data.keys())}")
        traceback.print_exc()
        return False