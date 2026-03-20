"""
Generate PDF documentation for District News AI Technical Deep Dive
"""
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.lib import colors
from datetime import datetime

def generate_pdf():
    # Create PDF
    pdf_path = "data/reports/District_News_AI_Technical_Deep_Dive.pdf"
    doc = SimpleDocTemplate(pdf_path, pagesize=A4, topMargin=0.5*inch, bottomMargin=0.5*inch)
    
    # Container for PDF elements
    elements = []
    
    # Define styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=6,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#2e5f9e'),
        spaceAfter=10,
        spaceBefore=12,
        fontName='Helvetica-Bold'
    )
    
    subheading_style = ParagraphStyle(
        'SubHeading',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=colors.HexColor('#3d6dbf'),
        spaceAfter=8,
        spaceBefore=10,
        fontName='Helvetica-Bold'
    )
    
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=9.5,
        alignment=TA_JUSTIFY,
        spaceAfter=10,
        fontName='Helvetica'
    )
    
    code_style = ParagraphStyle(
        'CodeStyle',
        parent=styles['BodyText'],
        fontSize=8,
        fontName='Courier',
        textColor=colors.HexColor('#333333'),
        leftIndent=20,
        spaceAfter=8,
        backColor=colors.HexColor('#f5f5f5')
    )
    
    # Title Page
    elements.append(Spacer(1, 0.8*inch))
    elements.append(Paragraph("District News AI", title_style))
    elements.append(Spacer(1, 0.1*inch))
    elements.append(Paragraph("Technical Deep Dive", heading_style))
    elements.append(Spacer(1, 0.3*inch))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}", body_style))
    elements.append(PageBreak())
    
    # Table of Contents
    elements.append(Paragraph("Table of Contents", heading_style))
    elements.append(Spacer(1, 0.15*inch))
    toc_items = [
        "1. System Architecture Overview",
        "2. Data Collection Strategy",
        "3. Text Processing & Location Resolution",
        "4. Analytics & Issue Detection",
        "5. Risk Prediction Model",
        "6. API Endpoints & Response Schema",
        "7. Policy Recommendation Engine",
        "8. Database Abstraction & Flexibility",
        "9. Scheduling & Operational Flow",
        "10. Performance & Scalability",
        "11. Key Design Decisions",
        "12. Integration Points & Extensibility"
    ]
    for item in toc_items:
        elements.append(Paragraph(item, body_style))
    elements.append(PageBreak())
    
    # Section 1
    elements.append(Paragraph("1. System Architecture Overview", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    elements.append(Paragraph(
        "The system follows a layered pipeline architecture: collectors fetch news from multiple sources, "
        "the processing layer cleans text and extracts geographic information, the database layer persists articles "
        "with embeddings and metadata, analytics compute issue clusters and risk scores, and the API layer exposes "
        "results to users. Each layer is independently scalable and can use different backend technologies.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    elements.append(Paragraph("<b>Core Components:</b>", subheading_style))
    elements.append(Spacer(1, 0.05*inch))
    
    components = [
        ("<b>Collector Layer:</b>", "NewsAPI, Google News RSS, GDELT, local publishers with 3-8 parallel workers"),
        ("<b>Processing Layer:</b>", "Text cleaning, NER location extraction, geo-resolution, embedding generation"),
        ("<b>Database Layer:</b>", "Supports SQLite, PostgreSQL, Neo4j, MongoDB with auto-detection"),
        ("<b>Analytics Engine:</b>", "Issue clustering, sentiment analysis, spike detection, risk scoring"),
        ("<b>API Layer:</b>", "FastAPI endpoints for district analysis, reports, and operational status")
    ]
    for label, desc in components:
        elements.append(Paragraph(f"{label} {desc}", body_style))
    elements.append(PageBreak())
    
    # Section 2
    elements.append(Paragraph("2. Data Collection Strategy", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    elements.append(Paragraph(
        "The system implements a multi-source collection strategy to maximize coverage of district-level civic issues. "
        "Collection runs every 60 minutes (or at a scheduled UTC hour) and deduplicates articles by URL to avoid redundancy.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    elements.append(Paragraph("<b>Collection Sources:</b>", subheading_style))
    
    sources = [
        "NewsAPI: 36 state queries + 700 district queries (5 pages × 100 articles per query)",
        "Google News RSS: 20 state + 300 district queries with optional date-range filtering",
        "GDELT: 30 state + 40 district queries with 6-hour lookback window",
        "Local Publishers: Custom scraping via RSS feeds or direct API integration"
    ]
    for source in sources:
        elements.append(Paragraph(f"• {source}", body_style))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Focus District Smart Sampling:</b>", subheading_style))
    elements.append(Paragraph(
        "Automatically selects 40 districts daily from under-covered regions by comparing master district list "
        "vs. already-assigned pair coverage. Prioritizes states with lowest coverage ratios to ensure equitable geographic spread.",
        body_style
    ))
    elements.append(PageBreak())
    
    # Section 3
    elements.append(Paragraph("3. Text Processing & Location Resolution", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    elements.append(Paragraph(
        "Raw articles undergo a four-stage processing pipeline to extract clean text and precise geographic mappings.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    processing_steps = [
        ("<b>Text Cleaning:</b>", "Remove HTML tags, normalize whitespace, lowercase, strip URLs and emails"),
        ("<b>Named Entity Recognition:</b>", "Batch extraction using spaCy (en_core_web_sm) with custom location tagging"),
        ("<b>Geo-Resolution:</b>", "Multi-pass matching against master district list with alias handling and confidence thresholding (default 0.45)"),
        ("<b>Embeddings:</b>", "Lazy-load sentence-transformers (all-MiniLM-L6-v2) for semantic downstream clustering")
    ]
    for step, desc in processing_steps:
        elements.append(Paragraph(f"{step} {desc}", body_style))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Database Storage:</b>", subheading_style))
    elements.append(Paragraph(
        "Each article is stored with: title, cleaned content, URL, source, publish timestamp, detected state/district, "
        "confidence scores, and semantic embeddings. This enables later semantic search and trend analysis.",
        body_style
    ))
    elements.append(PageBreak())
    
    # Section 4
    elements.append(Paragraph("4. Analytics & Issue Detection", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph("<b>Issue Classification:</b>", subheading_style))
    elements.append(Paragraph(
        "Keyword-based multi-category scoring across five civic domains: water, health, road_safety, crime, and infrastructure. "
        "The system counts keyword matches and assigns the issue with the highest match count. Fallback to 'other' category if "
        "no matches found.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Sentiment & Mood Scoring:</b>", subheading_style))
    elements.append(Paragraph(
        "Dictionary-based sentiment analysis using predefined word lists for positive (resolved, improved, ...), "
        "negative (accident, protest, violence, ...), and future-risk (warning, alert, forecast, ...) signals. "
        "Sentiment score ranges [-1, 1] and maps to tone distribution percentages.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Issue Clustering Algorithm:</b>", subheading_style))
    elements.append(Paragraph(
        "TF-IDF vectorization + MiniBatchKMeans clustering on article content. Cluster count = sqrt(number of articles). "
        "Each cluster receives a human-readable label from its top TF-IDF terms. Produces: cluster ID, problem label, "
        "frequency, trend (rising/stable/cooling), sentiment label, impact score, and risk score.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Spike Detection:</b>", subheading_style))
    elements.append(Paragraph(
        "Compares last 7 days article count vs 30-day daily average. Spike ratio > 2.0 triggers alert. "
        "Historical baselines tracked in separate table for cross-run consistency (90-day retention).",
        body_style
    ))
    elements.append(PageBreak())
    
    # Section 5
    elements.append(Paragraph("5. Risk Prediction Model", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph("<b>Feature Engineering:</b>", subheading_style))
    features_text = [
        "issue_spike_ratio - Recent vs baseline article frequency ratio",
        "negative_sentiment_score - Normalized negative word prevalence (0-1)",
        "protest_keyword_count - Raw count of protest-related terms",
        "issue_repetition_days - Days with that issue (capped at 30)",
        "hospital_density - Regional infrastructure proxy (0-2)",
        "rainfall - Environmental stress factor (mm)"
    ]
    for feat in features_text:
        elements.append(Paragraph(f"• {feat}", body_style))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Model Architecture:</b>", subheading_style))
    elements.append(Paragraph(
        "<b>Primary:</b> Trained ML model (LogisticRegression or RandomForest) if available.<br/>"
        "<b>Fallback:</b> Weighted heuristic combining normalized features: "
        "0.28×spike + 0.24×sentiment + 0.16×protest + 0.14×repetition + 0.12×infra + 0.06×rainfall",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Risk Level Mapping:</b>", subheading_style))
    risk_mapping = [
        "High: score ≥ 0.70 – Immediate escalation recommended",
        "Medium: 0.45 ≤ score < 0.70 – Monitor closely",
        "Low: score < 0.45 – Routine surveillance"
    ]
    for level in risk_mapping:
        elements.append(Paragraph(f"• {level}", body_style))
    elements.append(PageBreak())
    
    # Section 6
    elements.append(Paragraph("6. API Endpoints & Response Schema", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    elements.append(Paragraph(
        "FastAPI serves district intelligence through RESTful endpoints. All responses include metadata for audit and filtering.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    endpoints = [
        ("/analysis", "state, district", "Full analysis: top problems, trends, sentiment"),
        ("/reports/daily-summary", "state (opt), limit", "Prioritized districts with key issues"),
        ("/reports/daily-quality", "state (opt)", "Data quality metrics and confidence distribution"),
        ("/reports/source-mapping-audit", "state (opt), limit", "Source credibility audit"),
        ("/analysis/issues", "state, district", "Issue detail: spikes, sensitive events"),
        ("/analysis/public-mood", "state, district", "Sentiment breakdown and mood classification"),
        ("/health/pipeline", "—", "Operational status and pipeline metrics")
    ]
    elements.append(Paragraph("<b>Available Endpoints:</b>", subheading_style))
    for endpoint, params, purpose in endpoints:
        elements.append(Paragraph(f"<b>{endpoint}</b> ({params})<br/>{purpose}", body_style))
    elements.append(PageBreak())
    
    # Section 7
    elements.append(Paragraph("7. Policy Recommendation Engine", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    elements.append(Paragraph(
        "Maps detected primary issue + contextual signals (anger score, protest risk, sensitive events, infrastructure) "
        "to curated action lists. Outputs ≤8 recommendations ranked by urgency.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Issue-to-Action Mapping:</b>", subheading_style))
    actions = [
        "Water: emergency tankers, quality testing, filtration units",
        "Health: mobile medical units, critical care beds, screening camps",
        "Road Safety: pothole repair, speed cameras, improved lighting",
        "Crime: enforcement patrolling, grievance cells, conflict prevention",
        "Infrastructure: emergency maintenance budget, safety audits, war room"
    ]
    for action in actions:
        elements.append(Paragraph(f"• {action}", body_style))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Conditional Escalation:</b>", subheading_style))
    elements.append(Paragraph(
        "If anger_score > 0.6: add public communication and transparency actions.<br/>"
        "If protest_risk ≥ 0.7: escalate to 'urgent' and add coordination meeting + commitment timeline.",
        body_style
    ))
    elements.append(PageBreak())
    
    # Section 8
    elements.append(Paragraph("8. Database Abstraction & Flexibility", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph("<b>Supported Backends:</b>", subheading_style))
    db_backends = [
        "SQLite (default for local development)",
        "PostgreSQL (production, multi-user access)",
        "Neo4j (graph queries, relationship tracking)",
        "MongoDB (document-oriented, optional)"
    ]
    for backend in db_backends:
        elements.append(Paragraph(f"• {backend}", body_style))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Auto-Detection Logic:</b>", subheading_style))
    elements.append(Paragraph(
        "If DB_BACKEND env var set → use it; else if MONGODB_URI set → MongoDB; "
        "else if Neo4j port reachable → Neo4j (Docker); else if local SQLite exists → SQLite; else → Neo4j default.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Core Tables:</b>", subheading_style))
    elements.append(Paragraph(
        "<b>articles:</b> URL unique, TTL-based retention cleanup. "
        "<b>issue_history:</b> Daily aggregated counts by (date, state, district, issue). "
        "<b>pipeline_status:</b> Last run metadata for monitoring.",
        body_style
    ))
    elements.append(PageBreak())
    
    # Section 9
    elements.append(Paragraph("9. Scheduling & Operational Flow", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph("<b>Execution Modes:</b>", subheading_style))
    elements.append(Paragraph(
        "<b>Interval Mode:</b> Run every N minutes (default 60 mins). "
        "<b>Cron Mode:</b> Run daily at HH:00 UTC (default 06:00 = 11:30 IST)",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Daily Job Steps:</b>", subheading_style))
    job_steps = [
        "Ensure database ready",
        "Refresh issue history from recent articles",
        "Delete articles older than RETENTION_DAYS (default 5)",
        "Collect articles from all sources in parallel",
        "Deduplicate by URL or (title, source) pair",
        "Clean text, extract NER, resolve geography, generate embeddings",
        "Insert processed articles into database",
        "Backfill locations for articles with NER/geo failures",
        "Export daily summary report to JSON",
        "Update pipeline_status record with metrics"
    ]
    for i, step in enumerate(job_steps, 1):
        elements.append(Paragraph(f"{i}. {step}", body_style))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Artifacts Generated:</b>", subheading_style))
    elements.append(Paragraph(
        "<b>data/reports/daily_summary.json:</b> District rankings with top problems. "
        "<b>pipeline_status record:</b> Collected, inserted, backfilled, and unique article counts.",
        body_style
    ))
    elements.append(PageBreak())
    
    # Section 10
    elements.append(Paragraph("10. Performance & Scalability", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    
    perf_features = [
        ("<b>NER Batch Processing:</b>", 
         "Sentence-transformers lazily loaded; spaCy model cached to reduce memory footprint"),
        
        ("<b>Embedding Generation:</b>", 
         "Batch size 128; MiniBatchKMeans for ~10k articles to reduce memory"),
        
        ("<b>Parallel Collection:</b>", 
         "3–8 parallel workers via ThreadPoolExecutor; request timeouts 15–30 seconds"),
        
        ("<b>Incremental Clustering:</b>", 
         "MiniBatchKMeans algorithm; skips clustering if <4 articles per district"),
        
        ("<b>Data Retention:</b>", 
         "TTL-based auto-cleanup; 5-day rolling window by default"),
        
        ("<b>Low-Signal Handling:</b>", 
         "Confidence-weighted fallback aggregation for mixed-quality location data"),
        
        ("<b>Cold-Start Mitigation:</b>", 
         "Lazy model loading (not at startup) prevents deployment failures in resource-constrained environments")
    ]
    
    for feature, desc in perf_features:
        elements.append(Paragraph(f"{feature} {desc}", body_style))
    elements.append(PageBreak())
    
    # Section 11
    elements.append(Paragraph("11. Key Design Decisions", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    
    decisions = [
        ("<b>TF-IDF + KMeans (vs LLM):</b>", 
         "Interpretability, cost efficiency, low latency; keywords + clustering sufficient for civic classification"),
        
        ("<b>Heuristic Risk Model with Fallback:</b>", 
         "Fast inference without retraining; optional learnable model for advanced deployments"),
        
        ("<b>5-Day Rolling Retention:</b>", 
         "Balances freshness vs storage; issue history separately retained for 90 days"),
        
        ("<b>Confidence Scoring:</b>", 
         "Gracefully handles uncertain NER & geo-resolution; weighted aggregation for mixed-quality inputs"),
        
        ("<b>Multi-Source Collection:</b>", 
         "Redundancy and hyperlocal + national perspective coverage"),
        
        ("<b>District-Level Granularity:</b>", 
         "Aligns with admin boundaries; practical for policy response at execution level")
    ]
    
    for decision, rationale in decisions:
        elements.append(Paragraph(f"{decision} {rationale}", body_style))
    elements.append(PageBreak())
    
    # Section 12
    elements.append(Paragraph("12. Integration Points & Extensibility", heading_style))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph("<b>Adding Custom Issue Categories:</b>", subheading_style))
    elements.append(Paragraph(
        "Extend ISSUE_KEYWORDS, SENSITIVE_EVENT_KEYWORDS, and ISSUE_POLICY_MAP in "
        "analytics/keyword_packs.py; add custom aggregation routes in api/main.py.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Integrating External Data Sources:</b>", subheading_style))
    elements.append(Paragraph(
        "EPA rainfall data → feed risk model features; "
        "Hospital capacity data → refine health issue signals; "
        "Historical protest logs → train risk model with outcome labels.",
        body_style
    ))
    elements.append(Spacer(1, 0.15*inch))
    
    elements.append(Paragraph("<b>Scaling to Production:</b>", subheading_style))
    elements.append(Paragraph(
        "Switch to PostgreSQL/Neo4j backend; separate collectors & scheduler as microservices; "
        "add RabbitMQ/Celery for long-running tasks; cache popular queries in Redis.",
        body_style
    ))
    elements.append(Spacer(1, 0.5*inch))
    
    # Footer
    elements.append(Paragraph(
        f"Document Version: 1.0 | Last Updated: {datetime.now().strftime('%B %d, %Y at %H:%M UTC')}",
        body_style
    ))
    
    # Build PDF
    doc.build(elements)
    print(f"✓ PDF generated successfully: {pdf_path}")
    return pdf_path

if __name__ == "__main__":
    generate_pdf()
