import re
"""
rio_api.py — RIO PRINT MEDIA Sales Tracker v3.0
FastAPI backend replacing the PowerShell script.
Data stored in MongoDB Atlas.

Run locally:
    uvicorn rio_api:app --host 0.0.0.0 --port 8001 --reload

Deploy on Render.com:
    Start command: uvicorn rio_api:app --host 0.0.0.0 --port 8001
"""

import os, re, bcrypt, sys
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional, Any

from fastapi import FastAPI, Request, Query
from pydantic import BaseModel
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from bson import ObjectId
from dotenv import load_dotenv

# Use logging so uvicorn captures and displays output properly
import logging
import logging.handlers
import pathlib

# ── Logging setup ────────────────────────────────────────────────
# Windows (local run): writes to C:\Rio\Logs\rio_app.log
# Render / Linux:      stdout only (visible in Render → Logs tab)
import platform as _platform

_fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rio_api")
logger.setLevel(logging.DEBUG)

# Always add stdout handler (works everywhere)
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
logger.addHandler(_sh)

# On Windows: also write to C:\Rio\Logs\rio_app.log
_LOG_FILE = None
if _platform.system() == "Windows":
    _LOG_DIR = pathlib.Path(r"C:\Rio\Logs")
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        _LOG_FILE = _LOG_DIR / "rio_app.log"
        _fh = logging.handlers.RotatingFileHandler(
            _LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=2, encoding="utf-8"
        )
        _fh.setFormatter(_fmt)
        logger.addHandler(_fh)
        logger.info("=== RIO — Log file: %s ===", _LOG_FILE)
    except Exception as e:
        logger.warning("Could not create log file at C:\\Rio\\Logs: %s", e)
else:
    logger.info("=== RIO — Running on Linux/Render — logs go to stdout only ===")

load_dotenv(override=False)  # Never override Render environment variables

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
MONGO_URI  = os.environ.get("MONGO_URI", "")
MONGO_DB   = os.environ.get("MONGO_DB",  "RioPrintMedia")
HTML_FILE  = os.environ.get("HTML_FILE", "Rio_Sales_Tracker_ONLINE.html")

# ── Startup diagnostics ──
logger.info("=" * 60)
logger.info("RIO API STARTING UP")
logger.info(f"MONGO_DB  = {MONGO_DB}")
if MONGO_URI:
    import re as _re2
    _safe = _re2.sub(r':(.*?)@', ':***@', MONGO_URI)
    logger.info(f"MONGO_URI = SET → {_safe[:70]}")
else:
    logger.error("MONGO_URI = NOT SET — add it in Render Environment Variables!")
logger.info("=" * 60)

# ─────────────────────────────────────────────
#  DB
# ─────────────────────────────────────────────
_client: MongoClient = None
_db = None

def get_db():
    return _db

def col(name: str) -> Collection:
    return _db[name]

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def clean(doc: dict) -> dict:
    """Remove MongoDB _id and convert ObjectId."""
    if doc is None:
        return None
    doc.pop("_id", None)
    return doc

def clean_list(docs) -> list:
    return [clean(d) for d in docs]

def to_float(v, default=None):
    if v is None or v == "":
        return default
    try:
        return float(v)
    except:
        return default

def to_int(v, default=None):
    if v is None or v == "":
        return default
    try:
        return int(v)
    except:
        return default

def fy_from_date(d: str) -> str:
    """Return FY string like '2024-25' from a date string."""
    try:
        dt = datetime.strptime(d[:10], "%Y-%m-%d")
        m, y = dt.month, dt.year
        if m >= 4:
            return f"{y}-{str(y+1)[-2:]}"
        else:
            return f"{y-1}-{str(y)[-2:]}"
    except:
        return ""

def current_fy() -> str:
    return fy_from_date(datetime.now().strftime("%Y-%m-%d"))

def fy_range(fy: str):
    """Return (from_date, to_date) strings for a FY like '2024-25'."""
    try:
        y = int(fy.split("-")[0])
        return f"{y}-04-01", f"{y+1}-03-31"
    except:
        return None, None

def next_invoice_no(inv_type: str, fy: str) -> str:
    fy_from, fy_to = fy_range(fy)
    if inv_type == "GST":
        pipeline = [
            {"$match": {
                "InvoiceNo": {"$regex": r"^R\d"},
                "$or": [{"FY": fy}, {"$and": [{"FY": None}, {"InvoiceDate": {"$gte": fy_from, "$lte": fy_to}}]}]
            }},
            {"$project": {"num": {"$toInt": {"$substr": ["$InvoiceNo", 1, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("sales_invoices").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"R{n:02d}"
    else:
        pipeline = [
            {"$match": {
                "InvoiceNo": {"$regex": r"^RN"},
                "$or": [{"FY": fy}, {"$and": [{"FY": None}, {"InvoiceDate": {"$gte": fy_from, "$lte": fy_to}}]}]
            }},
            {"$project": {"num": {"$toInt": {"$substr": ["$InvoiceNo", 2, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("sales_invoices").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"RN{n:02d}"

def next_quotation_no(q_type: str, fy: str) -> str:
    fy_from, fy_to = fy_range(fy)
    if q_type == "GST":
        pipeline = [
            {"$match": {"QuotationNo": {"$regex": r"^Q\d"}, "QuotationDate": {"$gte": fy_from, "$lte": fy_to}}},
            {"$project": {"num": {"$toInt": {"$substr": ["$QuotationNo", 1, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("quotations").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"Q{n:02d}"
    else:
        pipeline = [
            {"$match": {"QuotationNo": {"$regex": r"^QN"}, "QuotationDate": {"$gte": fy_from, "$lte": fy_to}}},
            {"$project": {"num": {"$toInt": {"$substr": ["$QuotationNo", 2, 10]}}}},
            {"$group": {"_id": None, "max": {"$max": "$num"}}}
        ]
        res = list(col("quotations").aggregate(pipeline))
        n = (res[0]["max"] if res else 0) + 1
        return f"QN{n:02d}"

def next_product_code() -> str:
    pipeline = [
        {"$match": {"Code": {"$regex": r"^P"}}},
        {"$project": {"num": {"$toInt": {"$substr": ["$Code", 1, 10]}}}},
        {"$group": {"_id": None, "max": {"$max": "$num"}}}
    ]
    res = list(col("products").aggregate(pipeline))
    n = (res[0]["max"] if res else 0) + 1
    return f"P{n:03d}"

def next_id(collection_name: str, field: str = "Id") -> int:
    """Atomic ID generation using a counters collection."""
    result = _db["_counters"].find_one_and_update(
        {"_id": collection_name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True
    )
    return result["seq"]

def require_db():
    """Call at the start of every endpoint to ensure DB is ready."""
    if not ensure_db():
        raise Exception("Database not connected")

def init_indexes():
    """Create indexes for fast queries."""
    try:
        _db["sales_records"].create_index([("SNo", DESCENDING)])
        _db["daily_expenses"].create_index([("ExpDate", DESCENDING)])
        _db["sales_invoices"].create_index([("InvoiceDate", DESCENDING)])
        _db["quotations"].create_index([("QuotationDate", DESCENDING)])
        logger.info("Indexes created")
    except Exception as e:
        logger.warning(f"Index creation (non-fatal): {e}")

def init_counters():
    """Seed counters from current max IDs in each collection."""
    collections = [
        ("sales_records", "SNo"), ("daily_expenses", "Id"),
        ("notes", "Id"), ("followups", "Id"), ("rio_clients", "Id"),
        ("expense_categories", "Id"), ("jobs", "Id"), ("account_balances", "Id"),
        ("account_ledger", "Id"), ("products", "Id"), ("sales_invoices", "Id"),
        ("quotations", "Id"),
    ]
    for coll_name, field in collections:
        existing = _db["_counters"].find_one({"_id": coll_name})
        if not existing:
            pipeline = [{"$group": {"_id": None, "max": {"$max": f"${field}"}}}]
            res = list(_db[coll_name].aggregate(pipeline))
            max_val = to_int(res[0]["max"]) if res and res[0].get("max") is not None else 0
            _db["_counters"].update_one(
                {"_id": coll_name},
                {"$setOnInsert": {"seq": max_val}},
                upsert=True
            )

def set_sales_ledger_credits(sno: int, customer: str, job_name: str, payments: list):
    """Delete old ledger entries for a sales record and recreate them."""
    col("account_ledger").delete_many({"SalesRef": sno})
    for pay in payments:
        amt  = to_float(pay.get("Amt"))
        dt   = (pay.get("Date") or "").strip()
        mode = (pay.get("Mode") or "").strip()
        if not amt or amt <= 0 or not dt:
            continue
        acct_map = {
            "KVB MOM":     "KVB MOM",
            "KVB Mani":    "KVB Mani",
            "Indian Bank": "Indian Bank",
            "Cash":        "Cash Balance",
        }
        acct = acct_map.get(mode)
        if not acct:
            continue
        fy = fy_from_date(dt)
        if not fy:
            continue
        jn_str = f" — {job_name}" if job_name else ""
        desc = f"Sales: {customer}{jn_str}"
        last = col("account_ledger").find_one(
            {"AccountName": acct, "FY": fy},
            sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)]
        )
        prev_bal = to_float(last["Balance"]) if last else 0.0
        new_bal  = prev_bal + amt
        col("account_ledger").insert_one({
            "Id": next_id("account_ledger"), "AccountName": acct, "EntryDate": dt,
            "Description": desc, "CreditAmt": amt, "DebitAmt": 0,
            "Balance": new_bal, "EntryType": "Credit", "FY": fy,
            "ExpenseRef": None, "SalesRef": sno
        })

# ─────────────────────────────────────────────
#  APP STARTUP
# ─────────────────────────────────────────────
_db_connected = False  # track real connection state

def _connect_mongo():
    """Attempt MongoDB connection. Returns True on success, False on failure."""
    global _client, _db, _db_connected
    if not MONGO_URI:
        logger.error("=" * 60)
        logger.error("MONGO_URI IS NOT SET!")
        logger.error("Go to Render → your service → Environment → Add:")
        logger.error("  MONGO_URI = mongodb+srv://user:pass@cluster...")
        logger.error("  MONGO_DB  = RioPrintMedia")
        logger.error("Then click Save and Manual Deploy")
        logger.error("=" * 60)
        _db_connected = False
        return False
    # Mask password for safe logging
    safe_uri = MONGO_URI
    try:
        import re as _re
        safe_uri = _re.sub(r':(.*?)@', ':***@', MONGO_URI)
    except: pass
    logger.info(f"Connecting to MongoDB Atlas...")
    logger.info(f"URI: {safe_uri}")
    logger.info(f"DB:  {MONGO_DB}")
    try:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=25000,
            connectTimeoutMS=25000,
            socketTimeoutMS=30000,
            tls=True,
            retryWrites=True,
        )
        logger.info("MongoClient created, pinging Atlas...")
        _client.admin.command("ping")
        _db = _client[MONGO_DB]
        _db_connected = True
        logger.info(f"✓ MongoDB Atlas connected: {MONGO_DB}")
        return True
    except Exception as e:
        _db_connected = False
        err_str = str(e)
        logger.error(f"✗ MongoDB connection FAILED: {err_str}")
        if "Authentication failed" in err_str or "auth" in err_str.lower():
            logger.error("→ CHECK: Username and password in MONGO_URI")
            logger.error("→ Special chars in password must be URL-encoded (@ = %40)")
        elif "network" in err_str.lower() or "timeout" in err_str.lower() or "timed out" in err_str.lower():
            logger.error("→ CHECK: MongoDB Atlas Network Access")
            logger.error("→ Go to Atlas → Network Access → Add IP: 0.0.0.0/0 (Allow All)")
            logger.error("→ Render uses dynamic IPs, so 0.0.0.0/0 is required")
        elif "SSL" in err_str or "TLS" in err_str:
            logger.error("→ SSL/TLS error — check Atlas cluster TLS settings")
        logger.error(f"→ URI used (masked): {safe_uri[:60]}...")
        return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # DO NOT raise — keep server alive even if DB is temporarily down.
    # Render cold starts can be slow; server retries on first real request.
    connected = _connect_mongo()
    if connected:
        try:
            init_indexes()
            logger.info("Indexes ready")
        except Exception as e:
            logger.warning(f"init_indexes error (non-fatal): {e}")
        try:
            init_counters()
            logger.info("Counters initialised")
        except Exception as e:
            logger.warning(f"init_counters error (non-fatal): {e}")
        try:
            ensure_default_users()
            logger.info("Users ready")
        except Exception as e:
            logger.warning(f"ensure_default_users error (non-fatal): {e}")
    else:
        logger.warning("Server started WITHOUT DB — will retry on first request")
    yield
    if _client:
        _client.close()
        logger.info("MongoDB connection closed")

app = FastAPI(title="Rio Print Media API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def ok(data=None, **kwargs):
    if data is not None:
        return JSONResponse(content=data)
    return JSONResponse(content={"ok": True, **kwargs})

def err(msg, status=400):
    return JSONResponse(content={"error": msg}, status_code=status)

# ─────────────────────────────────────────────
#  LIVE HTML PATCHER — applied every request
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
#  SERVE HTML DASHBOARD
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    logger.info(f"GET / — serving {HTML_FILE}")
    if not os.path.exists(HTML_FILE):
        logger.error(f"HTML file not found: {HTML_FILE} — cwd={os.getcwd()}")
        return HTMLResponse(f"<h2>File not found: {HTML_FILE}</h2><p>CWD: {os.getcwd()}</p><p>Files: {os.listdir('.')[:20]}</p>", 404)
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()
    logger.info(f"Serving {len(html)} bytes")
    return HTMLResponse(html)

# ─────────────────────────────────────────────
#  MOBILE APP
# ─────────────────────────────────────────────
MOBILE_PIN = os.environ.get("MOBILE_PIN", "4104")
RENDER_URL = os.environ.get("RENDER_URL", "https://rio-print-media.onrender.com")

@app.get("/mobile", response_class=HTMLResponse)
async def serve_mobile(request: Request):
    api_url = RENDER_URL
    pin = MOBILE_PIN
    # Compute current FY server-side to avoid f-string colon issues
    _now = datetime.now()
    _m, _y = _now.month, _now.year
    current_fy = f"{_y}-{str(_y+1)[2:]}" if _m >= 4 else f"{_y-1}-{str(_y)[2:]}"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#0d1b3e">
<title>RIO PRINT MEDIA</title>
<link href="https://fonts.googleapis.com/css2?family=Exo+2:wght@500;600;700;800&family=Nunito:wght@400;600;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;}}
:root{{--blue:#1a237e;--teal:#00796b;--orange:#ef6c00;--red:#c62828;--green:#2e7d32;--purple:#7b1fa2;--pink:#ad1457;--accent:#00b8d9;}}
body{{font-family:'Nunito',sans-serif;background:#f0f2f8;overflow-x:hidden;}}
input,select,textarea,button{{font-family:'Exo 2',sans-serif;}}

/* PIN SCREEN */
#pin-screen{{position:fixed;inset:0;background:linear-gradient(135deg,#0e1220,#1a237e);display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:9999;}}
.pin-logo{{font-family:'Exo 2',sans-serif;font-size:1.1rem;font-weight:900;color:white;letter-spacing:3px;margin-bottom:8px;opacity:0.9;}}
.pin-sub{{font-size:0.75rem;color:rgba(255,255,255,0.5);margin-bottom:32px;letter-spacing:1px;}}
.pin-dots{{display:flex;gap:14px;margin-bottom:24px;}}
.pin-dot{{width:16px;height:16px;border-radius:50%;border:2px solid rgba(255,255,255,0.5);background:transparent;transition:background 0.2s;}}
.pin-dot.filled{{background:white;border-color:white;}}
.pin-err{{color:#ef9a9a;font-size:0.78rem;min-height:20px;margin-bottom:12px;text-align:center;}}
.pin-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;width:240px;}}
.pin-btn{{height:64px;border-radius:16px;border:none;background:rgba(255,255,255,0.12);color:white;font-size:1.4rem;font-weight:700;cursor:pointer;transition:background 0.15s,transform 0.1s;}}
.pin-btn:active{{background:rgba(255,255,255,0.25);transform:scale(0.95);}}
.pin-btn.del{{font-size:1rem;}}
.pin-btn.empty{{background:transparent;pointer-events:none;}}

/* TOP BAR */
#top-bar{{position:fixed;top:0;left:0;right:0;height:52px;background:linear-gradient(90deg,#0e1220,#1a237e);display:flex;align-items:center;padding:0 12px;gap:10px;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,0.3);}}
#top-bar .title{{color:white;font-family:'Exo 2',sans-serif;font-weight:800;font-size:0.9rem;letter-spacing:1px;flex:1;}}
.status-dot{{width:8px;height:8px;border-radius:50%;background:#ef9a9a;}}
.status-dot.connected{{background:#43a047;}}

/* MAIN CONTENT */
#main{{padding-top:52px;padding-bottom:60px;min-height:100vh;}}

/* BOTTOM NAV */
#bottom-nav{{position:fixed;bottom:0;left:0;right:0;height:56px;background:white;border-top:1px solid #e0e0e0;display:flex;z-index:100;}}
.nav-item{{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:2px;cursor:pointer;border:none;background:transparent;padding:4px 0;transition:background 0.15s;}}
.nav-item.active{{background:#e8eaf6;}}
.nav-item svg{{width:20px;height:20px;fill:#9e9e9e;}}
.nav-item.active svg{{fill:#1a237e;}}
.nav-item span{{font-size:9px;color:#9e9e9e;font-family:'Exo 2',sans-serif;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;}}
.nav-item.active span{{color:#1a237e;}}

/* PANELS */
.panel{{display:none;padding:12px;}}
.panel.active{{display:block;}}

/* CARDS */
.stat-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px;}}
.stat-card{{background:white;border-radius:14px;padding:14px;border-left:4px solid #ccc;}}
.stat-card.blue{{border-left-color:#5c6bc0;}}
.stat-card.red{{border-left-color:#c62828;}}
.stat-card.orange{{border-left-color:#ef6c00;}}
.stat-card.teal{{border-left-color:#00796b;}}
.stat-label{{font-size:10px;color:#888;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;font-family:'Exo 2',sans-serif;font-weight:700;}}
.stat-val{{font-family:'Exo 2',sans-serif;font-size:1.3rem;font-weight:800;color:#1a1a2e;}}
.stat-sub{{font-size:10px;color:#aaa;margin-top:3px;}}

/* SECTION HEADER */
.sec-header{{font-family:'Exo 2',sans-serif;font-size:0.78rem;font-weight:800;color:#555;text-transform:uppercase;letter-spacing:0.8px;margin:14px 0 8px;display:flex;align-items:center;justify-content:space-between;}}

/* LIST ITEMS */
.list-item{{background:white;border-radius:12px;padding:12px 14px;margin-bottom:8px;border:1px solid #f0f0f0;}}
.list-item-top{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px;}}
.list-item-name{{font-family:'Exo 2',sans-serif;font-size:0.85rem;font-weight:700;color:#1a1a2e;}}
.list-item-amount{{font-family:'Exo 2',sans-serif;font-size:0.9rem;font-weight:800;color:#1565c0;}}
.list-item-amount.red{{color:#c62828;}}
.list-item-meta{{font-size:0.72rem;color:#888;display:flex;gap:8px;flex-wrap:wrap;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:20px;font-size:0.65rem;font-family:'Exo 2',sans-serif;font-weight:700;}}
.badge-blue{{background:#e8eaf6;color:#1a237e;}}
.badge-green{{background:#e8f5e9;color:#2e7d32;}}
.badge-red{{background:#ffebee;color:#c62828;}}
.badge-orange{{background:#fff3e0;color:#e65100;}}

/* FORM */
.form-card{{background:white;border-radius:14px;padding:14px;margin-bottom:12px;}}
.form-card h3{{font-family:'Exo 2',sans-serif;font-size:0.82rem;font-weight:800;color:#1a237e;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;}}
.field{{margin-bottom:10px;}}
.field label{{display:block;font-size:0.72rem;font-weight:700;color:#666;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;}}
.field input,.field select,.field textarea{{width:100%;padding:10px 12px;border:1.5px solid #e0e0e0;border-radius:10px;font-size:0.9rem;outline:none;background:#fafafa;transition:border 0.2s;}}
.field input:focus,.field select:focus{{border-color:#3949ab;background:white;}}
.field-row{{display:grid;grid-template-columns:1fr 1fr;gap:10px;}}
.btn-primary{{width:100%;padding:13px;background:linear-gradient(90deg,#1a237e,#3949ab);color:white;border:none;border-radius:12px;font-size:0.95rem;font-family:'Exo 2',sans-serif;font-weight:700;cursor:pointer;letter-spacing:0.5px;margin-top:4px;}}
.btn-primary:active{{opacity:0.85;transform:scale(0.98);}}
.btn-secondary{{width:100%;padding:11px;background:#f5f5f5;color:#555;border:1px solid #e0e0e0;border-radius:12px;font-size:0.85rem;font-family:'Exo 2',sans-serif;font-weight:700;cursor:pointer;margin-top:8px;}}

/* LOADING */
.loading{{text-align:center;padding:40px;color:#aaa;font-size:0.85rem;}}
.empty{{text-align:center;padding:40px;color:#aaa;font-size:0.85rem;}}

/* FAB */
.fab{{position:fixed;bottom:68px;right:16px;width:52px;height:52px;border-radius:50%;background:linear-gradient(135deg,#1a237e,#3949ab);color:white;border:none;font-size:1.5rem;display:flex;align-items:center;justify-content:center;cursor:pointer;box-shadow:0 4px 14px rgba(26,35,126,0.4);z-index:99;}}
.fab:active{{transform:scale(0.92);}}

/* MODAL */
.modal-overlay{{position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:200;display:none;align-items:flex-end;justify-content:center;}}
.modal-overlay.open{{display:flex;}}
.modal{{background:white;border-radius:20px 20px 0 0;width:100%;max-height:85vh;overflow-y:auto;padding:20px 16px 32px;}}
.modal-handle{{width:36px;height:4px;background:#e0e0e0;border-radius:2px;margin:0 auto 16px;}}
.modal h2{{font-family:'Exo 2',sans-serif;font-size:1rem;font-weight:800;color:#1a237e;margin-bottom:16px;}}
</style>
</head>
<body>

<!-- PIN SCREEN -->
<div id="pin-screen">
  <div class="pin-logo">&#128424; RIO PRINT MEDIA</div>
  <div class="pin-sub">MOBILE DASHBOARD</div>
  <div class="pin-dots">
    <div class="pin-dot" id="d0"></div>
    <div class="pin-dot" id="d1"></div>
    <div class="pin-dot" id="d2"></div>
    <div class="pin-dot" id="d3"></div>
  </div>
  <div class="pin-err" id="pin-err"></div>
  <div class="pin-grid">
    <button class="pin-btn" onclick="pinPress('1')">1</button>
    <button class="pin-btn" onclick="pinPress('2')">2</button>
    <button class="pin-btn" onclick="pinPress('3')">3</button>
    <button class="pin-btn" onclick="pinPress('4')">4</button>
    <button class="pin-btn" onclick="pinPress('5')">5</button>
    <button class="pin-btn" onclick="pinPress('6')">6</button>
    <button class="pin-btn" onclick="pinPress('7')">7</button>
    <button class="pin-btn" onclick="pinPress('8')">8</button>
    <button class="pin-btn" onclick="pinPress('9')">9</button>
    <button class="pin-btn empty"></button>
    <button class="pin-btn" onclick="pinPress('0')">0</button>
    <button class="pin-btn del" onclick="pinDel()">&#9003;</button>
  </div>
</div>

<!-- TOP BAR -->
<div id="top-bar" style="display:none">
  <span class="title">RIO PRINT MEDIA</span>
  <div class="status-dot" id="status-dot"></div>
</div>

<!-- MAIN CONTENT -->
<div id="main" style="display:none">

  <!-- SUMMARY PANEL -->
  <div class="panel active" id="panel-summary">
    <div class="stat-grid">
      <div class="stat-card blue"><div class="stat-label">Total Sales</div><div class="stat-val" id="m-total-sales">...</div><div class="stat-sub" id="m-sales-count"></div></div>
      <div class="stat-card red"><div class="stat-label">Pending</div><div class="stat-val" id="m-pending">...</div><div class="stat-sub" id="m-pending-count"></div></div>
      <div class="stat-card orange"><div class="stat-label">Expenses</div><div class="stat-val" id="m-expenses">...</div><div class="stat-sub" id="m-exp-count"></div></div>
      <div class="stat-card teal"><div class="stat-label">Received</div><div class="stat-val" id="m-received">...</div><div class="stat-sub">this FY</div></div>
    </div>
    <div class="sec-header">Recent Sales</div>
    <div id="m-recent-sales"><div class="loading">Loading...</div></div>
  </div>

  <!-- SALES PANEL -->
  <div class="panel" id="panel-sales">
    <div class="sec-header">Sales Records <span id="m-sales-badge" class="badge badge-blue"></span></div>
    <div id="m-sales-list"><div class="loading">Loading...</div></div>
  </div>

  <!-- EXPENSES PANEL -->
  <div class="panel" id="panel-expenses">
    <div class="sec-header">Expenses <span id="m-exp-badge" class="badge badge-orange"></span></div>
    <div id="m-exp-list"><div class="loading">Loading...</div></div>
  </div>

  <!-- PENDING PANEL -->
  <div class="panel" id="panel-pending">
    <div class="sec-header">Pending Payments</div>
    <div id="m-pending-list"><div class="loading">Loading...</div></div>
  </div>

  <!-- BILLING PANEL -->
  <div class="panel" id="panel-billing">
    <div class="sec-header">Recent Invoices</div>
    <div id="m-invoice-list"><div class="loading">Loading...</div></div>
  </div>

</div>

<!-- BOTTOM NAV -->
<div id="bottom-nav" style="display:none">
  <button class="nav-item active" onclick="switchTab('summary',this)">
    <svg viewBox="0 0 24 24"><path d="M3 13h8V3H3v10zm0 8h8v-6H3v6zm10 0h8V11h-8v10zm0-18v6h8V3h-8z"/></svg>
    <span>Summary</span>
  </button>
  <button class="nav-item" onclick="switchTab('sales',this)">
    <svg viewBox="0 0 24 24"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-7 14l-5-5 1.41-1.41L12 14.17l7.59-7.59L21 8l-9 9z"/></svg>
    <span>Sales</span>
  </button>
  <button class="nav-item" onclick="switchTab('expenses',this)">
    <svg viewBox="0 0 24 24"><path d="M11.8 10.9c-2.27-.59-3-1.2-3-2.15 0-1.09 1.01-1.85 2.7-1.85 1.78 0 2.44.85 2.5 2.1h2.21c-.07-1.72-1.12-3.3-3.21-3.81V3h-3v2.16c-1.94.42-3.5 1.68-3.5 3.61 0 2.31 1.91 3.46 4.7 4.13 2.5.6 3 1.48 3 2.41 0 .69-.49 1.79-2.7 1.79-2.06 0-2.87-.92-2.98-2.1h-2.2c.12 2.19 1.76 3.42 3.68 3.83V21h3v-2.15c1.95-.37 3.5-1.5 3.5-3.55 0-2.84-2.43-3.81-4.7-4.4z"/></svg>
    <span>Expenses</span>
  </button>
  <button class="nav-item" onclick="switchTab('pending',this)">
    <svg viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg>
    <span>Pending</span>
  </button>
  <button class="nav-item" onclick="switchTab('billing',this)">
    <svg viewBox="0 0 24 24"><path d="M20 4H4c-1.11 0-2 .89-2 2v12c0 1.11.89 2 2 2h16c1.11 0 2-.89 2-2V6c0-1.11-.89-2-2-2zm0 14H4v-6h16v6zm0-10H4V6h16v2z"/></svg>
    <span>Billing</span>
  </button>
</div>

<!-- ADD SALE MODAL -->
<div class="modal-overlay" id="modal-sale">
  <div class="modal">
    <div class="modal-handle"></div>
    <h2>&#43; New Sale</h2>
    <div class="field"><label>Customer</label><input id="s-customer" type="text" list="m-customers" placeholder="Customer name"></div>
    <datalist id="m-customers"></datalist>
    <div class="field-row">
      <div class="field"><label>Category</label>
        <select id="s-category">
          <option>Flex</option><option>Sunpac</option><option>Banner</option>
          <option>Sticker</option><option>Offset</option><option>Digital</option><option>Other</option>
        </select>
      </div>
      <div class="field"><label>Order Date</label><input id="s-date" type="date"></div>
    </div>
    <div class="field"><label>Product / Description</label><input id="s-product" type="text" placeholder="Product description"></div>
    <div class="field-row">
      <div class="field"><label>Total Amount</label><input id="s-total" type="number" placeholder="0.00"></div>
      <div class="field"><label>Advance Paid</label><input id="s-advance" type="number" placeholder="0.00"></div>
    </div>
    <div class="field"><label>Billing Type</label>
      <select id="s-billing"><option>GST</option><option>NON-GST</option><option>IGST</option></select>
    </div>
    <div id="s-err" style="color:#c62828;font-size:0.78rem;min-height:16px;margin:4px 0;"></div>
    <button class="btn-primary" onclick="saveSale()">Save Sale</button>
    <button class="btn-secondary" onclick="closeModal('modal-sale')">Cancel</button>
  </div>
</div>

<!-- ADD EXPENSE MODAL -->
<div class="modal-overlay" id="modal-expense">
  <div class="modal">
    <div class="modal-handle"></div>
    <h2>&#43; New Expense</h2>
    <div class="field"><label>Date</label><input id="e-date" type="date"></div>
    <div class="field"><label>Category</label>
      <select id="e-cat"><option>Loading...</option></select>
    </div>
    <div class="field"><label>Description</label><input id="e-desc" type="text" placeholder="Expense description"></div>
    <div class="field"><label>Amount</label><input id="e-amount" type="number" placeholder="0.00"></div>
    <div class="field"><label>Paid By</label>
      <select id="e-paid"><option>Cash</option><option>KVB MOM</option><option>KVB Mani</option><option>Indian Bank</option></select>
    </div>
    <div id="e-err" style="color:#c62828;font-size:0.78rem;min-height:16px;margin:4px 0;"></div>
    <button class="btn-primary" onclick="saveExpense()">Save Expense</button>
    <button class="btn-secondary" onclick="closeModal('modal-expense')">Cancel</button>
  </div>
</div>

<!-- FAB -->
<button class="fab" id="fab" onclick="openFab()" style="display:none">&#43;</button>

<script>
const API = '{api_url}/api';
const CORRECT_PIN = '{pin}';
var _pinVal = '';
var _pinAttempts = 0;
var _pinLocked = false;
var _data = {{ sales:[], expenses:[], clients:[] }};
var _activeTab = 'summary';
var _expCategories = [];

// ── PIN ──────────────────────────────────────
function pinPress(d) {{
  if (_pinLocked) return;
  if (_pinVal.length >= 4) return;
  _pinVal += d;
  updateDots();
  if (_pinVal.length === 4) setTimeout(checkPin, 150);
}}
function pinDel() {{
  _pinVal = _pinVal.slice(0,-1);
  updateDots();
  document.getElementById('pin-err').textContent = '';
}}
function updateDots() {{
  for (var i=0;i<4;i++) {{
    document.getElementById('d'+i).classList.toggle('filled', i < _pinVal.length);
  }}
}}
function checkPin() {{
  if (_pinVal === CORRECT_PIN) {{
    document.getElementById('pin-screen').style.display = 'none';
    document.getElementById('top-bar').style.display = 'flex';
    document.getElementById('main').style.display = 'block';
    document.getElementById('bottom-nav').style.display = 'flex';
    document.getElementById('fab').style.display = 'flex';
    sessionStorage.setItem('rio_mobile_auth','1');
    loadAllData();
  }} else {{
    _pinAttempts++;
    _pinVal = '';
    updateDots();
    if (_pinAttempts >= 3) {{
      _pinLocked = true;
      document.getElementById('pin-err').textContent = 'Too many attempts. Wait 30s.';
      setTimeout(function(){{ _pinLocked=false; _pinAttempts=0; document.getElementById('pin-err').textContent=''; }}, 30000);
    }} else {{
      document.getElementById('pin-err').textContent = 'Wrong PIN. ' + (3-_pinAttempts) + ' attempts left.';
    }}
  }}
}}

// ── INIT ─────────────────────────────────────
window.onload = function() {{
  if (sessionStorage.getItem('rio_mobile_auth')==='1') {{
    document.getElementById('pin-screen').style.display='none';
    document.getElementById('top-bar').style.display='flex';
    document.getElementById('main').style.display='block';
    document.getElementById('bottom-nav').style.display='flex';
    document.getElementById('fab').style.display='flex';
    loadAllData();
  }}
  // Set today's date on forms
  var today = new Date().toISOString().split('T')[0];
  document.getElementById('s-date').value = today;
  document.getElementById('e-date').value = today;
}};

// ── TAB SWITCHING ─────────────────────────────
function switchTab(tab, el) {{
  _activeTab = tab;
  document.querySelectorAll('.panel').forEach(function(p){{ p.classList.remove('active'); }});
  document.getElementById('panel-'+tab).classList.add('active');
  document.querySelectorAll('.nav-item').forEach(function(n){{ n.classList.remove('active'); }});
  el.classList.add('active');
}}

// ── LOAD DATA ─────────────────────────────────
async function loadAllData() {{
  try {{
    var r = await fetch(API+'/ping');
    var dot = document.getElementById('status-dot');
    if (r.ok) {{ dot.className='status-dot connected'; }} else {{ dot.className='status-dot'; }}
  }} catch(e) {{ document.getElementById('status-dot').className='status-dot'; }}

  try {{
    var [salesR, expR, clientsR] = await Promise.all([
      fetch(API+'/sales').then(r=>r.json()),
      fetch(API+'/expenses').then(r=>r.json()),
      fetch(API+'/rio_clients').then(r=>r.json()).catch(()=>[])
    ]);
    _data.sales = salesR || [];
    _data.expenses = expR || [];
    _data.clients = Array.isArray(clientsR) ? clientsR : [];
    renderAll();
    loadExpCategories();
    loadInvoices();
  }} catch(e) {{ console.error('loadAllData',e); }}
}}

async function loadExpCategories() {{
  try {{
    var r = await fetch(API+'/expense_categories');
    var d = await r.json();
    _expCategories = d || [];
    var sel = document.getElementById('e-cat');
    sel.innerHTML = _expCategories.map(function(c){{
      return '<option>'+c.CategoryName+'</option>';
    }}).join('') || '<option>General</option>';
  }} catch(e) {{}}
}}

async function loadInvoices() {{
  try {{
    var r = await fetch(API+'/billing/invoices?pageSize=20&from=2025-04-01&to=2026-03-31');
    var d = await r.json();
    var rows = (d.data || []);
    var html = rows.length ? rows.map(function(inv){{
      return '<div class="list-item">'
        +'<div class="list-item-top"><span class="list-item-name">'+inv.CustomerName+'</span>'
        +'<span class="list-item-amount">&#8377;'+fmt(inv.TotalAmount)+'</span></div>'
        +'<div class="list-item-meta"><span>'+inv.InvoiceNo+'</span><span>'+inv.InvoiceDate+'</span>'
        +'<span class="badge badge-blue">'+inv.BillingType+'</span></div></div>';
    }}).join('') : '<div class="empty">No invoices</div>';
    document.getElementById('m-invoice-list').innerHTML = html;
  }} catch(e) {{}}
}}

function renderAll() {{
  var sales = _data.sales;
  var exps = _data.expenses;

  // Summary stats
  var totalAmt = sales.reduce(function(s,r){{ return s+(parseFloat(r.TotalAmount)||0); }},0);
  var received = sales.reduce(function(s,r){{ return s+(parseFloat(r.ReceivedAmount)||0); }},0);
  var pending = totalAmt - received;
  var pendingCount = sales.filter(function(r){{ return (parseFloat(r.RemainingAmount)||0)>0; }}).length;
  var totalExp = exps.reduce(function(s,e){{ return s+(parseFloat(e.Amount)||0); }},0);

  document.getElementById('m-total-sales').textContent = '&#8377;'+fmtL(totalAmt);
  document.getElementById('m-sales-count').textContent = sales.length+' records';
  document.getElementById('m-pending').textContent = '&#8377;'+fmtL(pending);
  document.getElementById('m-pending-count').textContent = pendingCount+' unpaid';
  document.getElementById('m-expenses').textContent = '&#8377;'+fmtL(totalExp);
  document.getElementById('m-exp-count').textContent = exps.length+' entries';
  document.getElementById('m-received').textContent = '&#8377;'+fmtL(received);

  // Recent sales (top 5)
  var recent = sales.slice(0,5);
  document.getElementById('m-recent-sales').innerHTML = recent.length ? recent.map(saleCard).join('') : '<div class="empty">No sales yet</div>';

  // Sales list
  document.getElementById('m-sales-badge').textContent = sales.length;
  document.getElementById('m-sales-list').innerHTML = sales.length ? sales.map(saleCard).join('') : '<div class="empty">No sales</div>';

  // Expenses list
  document.getElementById('m-exp-badge').textContent = exps.length;
  document.getElementById('m-exp-list').innerHTML = exps.length ? exps.slice(0,50).map(expCard).join('') : '<div class="empty">No expenses</div>';

  // Pending list
  var pendingList = sales.filter(function(r){{ return (parseFloat(r.RemainingAmount)||0)>0; }});
  document.getElementById('m-pending-list').innerHTML = pendingList.length ? pendingList.map(function(r){{
    return '<div class="list-item">'
      +'<div class="list-item-top"><span class="list-item-name">'+r.Customer+'</span>'
      +'<span class="list-item-amount red">&#8377;'+fmt(r.RemainingAmount)+'</span></div>'
      +'<div class="list-item-meta"><span>SNo: '+r.SNo+'</span><span>'+r.OrderDate+'</span>'
      +'<span class="badge badge-red">PENDING</span></div></div>';
  }}).join('') : '<div class="empty">No pending payments</div>';

  // Populate customer datalist
  var dl = document.getElementById('m-customers');
  dl.innerHTML = _data.clients.map(function(c){{ return '<option value="'+c+'">'; }}).join('');
}}

function saleCard(r) {{
  var pending = parseFloat(r.RemainingAmount)||0;
  return '<div class="list-item">'
    +'<div class="list-item-top"><span class="list-item-name">'+r.Customer+'</span>'
    +'<span class="list-item-amount">&#8377;'+fmt(r.TotalAmount)+'</span></div>'
    +'<div class="list-item-meta"><span>'+r.OrderDate+'</span><span class="badge badge-blue">'+r.Category+'</span>'
    +(pending>0?'<span class="badge badge-red">Pending &#8377;'+fmt(pending)+'</span>':'<span class="badge badge-green">Paid</span>')
    +'</div></div>';
}}

function expCard(e) {{
  return '<div class="list-item">'
    +'<div class="list-item-top"><span class="list-item-name">'+e.Description+'</span>'
    +'<span class="list-item-amount">&#8377;'+fmt(e.Amount)+'</span></div>'
    +'<div class="list-item-meta"><span>'+e.Date+'</span><span class="badge badge-orange">'+e.Category+'</span></div></div>';
}}

// ── FAB ───────────────────────────────────────
function openFab() {{
  if (_activeTab==='sales'||_activeTab==='summary') openModal('modal-sale');
  else if (_activeTab==='expenses') openModal('modal-expense');
  else if (_activeTab==='billing') window.location.href='/';
}}

// ── MODALS ────────────────────────────────────
function openModal(id) {{ document.getElementById(id).classList.add('open'); }}
function closeModal(id) {{ document.getElementById(id).classList.remove('open'); }}

// ── SAVE SALE ─────────────────────────────────
async function saveSale() {{
  var customer = document.getElementById('s-customer').value.trim();
  var total = parseFloat(document.getElementById('s-total').value)||0;
  var errEl = document.getElementById('s-err');
  if (!customer) {{ errEl.textContent='Customer required'; return; }}
  if (!total) {{ errEl.textContent='Amount required'; return; }}
  errEl.textContent='Saving...';
  try {{
    var advance = parseFloat(document.getElementById('s-advance').value)||0;
    var body = {{
      Customer: customer,
      Category: document.getElementById('s-category').value,
      OrderDate: document.getElementById('s-date').value,
      ProductSize: document.getElementById('s-product').value,
      BillingType: document.getElementById('s-billing').value,
      TotalAmount: total,
      Payment1Amt: advance,
      Payment1Mode: 'Cash',
      Payment1Date: document.getElementById('s-date').value,
      FY: '{current_fy}'
    }};
    var r = await fetch(API+'/sales', {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(body)}});
    var d = await r.json();
    if (r.ok) {{
      errEl.textContent='';
      closeModal('modal-sale');
      document.getElementById('s-customer').value='';
      document.getElementById('s-total').value='';
      document.getElementById('s-advance').value='';
      document.getElementById('s-product').value='';
      loadAllData();
    }} else {{ errEl.textContent = d.error||'Save failed'; }}
  }} catch(e) {{ errEl.textContent='Connection error'; }}
}}

// ── SAVE EXPENSE ──────────────────────────────
async function saveExpense() {{
  var desc = document.getElementById('e-desc').value.trim();
  var amount = parseFloat(document.getElementById('e-amount').value)||0;
  var errEl = document.getElementById('e-err');
  if (!desc) {{ errEl.textContent='Description required'; return; }}
  if (!amount) {{ errEl.textContent='Amount required'; return; }}
  errEl.textContent='Saving...';
  try {{
    var body = {{
      Date: document.getElementById('e-date').value,
      Category: document.getElementById('e-cat').value,
      Description: desc,
      Amount: amount,
      PaidBy: document.getElementById('e-paid').value
    }};
    var r = await fetch(API+'/expenses', {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(body)}});
    var d = await r.json();
    if (r.ok) {{
      errEl.textContent='';
      closeModal('modal-expense');
      document.getElementById('e-desc').value='';
      document.getElementById('e-amount').value='';
      loadAllData();
    }} else {{ errEl.textContent = d.error||'Save failed'; }}
  }} catch(e) {{ errEl.textContent='Connection error'; }}
}}

// ── FORMATTERS ────────────────────────────────
function fmt(v) {{
  var n = parseFloat(v)||0;
  return n.toLocaleString('en-IN',{{minimumFractionDigits:0,maximumFractionDigits:0}});
}}
function fmtL(v) {{
  var n = parseFloat(v)||0;
  if (n>=100000) return (n/100000).toFixed(1)+'L';
  if (n>=1000) return (n/1000).toFixed(0)+'K';
  return n.toFixed(0);
}}
</script>
</body>
</html>"""
    return HTMLResponse(html)

# ─────────────────────────────────────────────
#  DB RECONNECT HELPER
# ─────────────────────────────────────────────
def ensure_db():
    """If DB not connected, attempt reconnect. Returns True if connected."""
    global _db_connected
    if _db_connected and _db is not None:
        return True
    logger.warning("DB not connected — attempting reconnect...")
    connected = _connect_mongo()
    if connected:
        try: init_counters()
        except: pass
        try: ensure_default_users()
        except: pass
    return connected

# ─────────────────────────────────────────────
#  PING — reports real DB status
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
#  CLIENT-SIDE LOG ENDPOINT
#  HTML sends log entries here; we write to C:\Rio\Logs\rio_app.log
# ─────────────────────────────────────────────
@app.post("/api/log")
async def client_log(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(content={"ok": False}, status_code=400)
    level   = str(body.get("level",  "INFO")).upper()
    user    = str(body.get("user",   "unknown"))
    action  = str(body.get("action", ""))
    detail  = str(body.get("detail", ""))
    msg = f"[CLIENT] user={user} | {action} | {detail}"
    if level == "ERROR":
        logger.error(msg)
    elif level == "WARN":
        logger.warning(msg)
    else:
        logger.info(msg)
    return {"ok": True}


@app.get("/api/ping")
async def ping():
    connected = ensure_db()
    if not connected:
        return JSONResponse(
            content={"ok": False, "error": "MongoDB not connected. Check MONGO_URI on Render.", "db": MONGO_DB},
            status_code=503
        )
    try:
        _client.admin.command("ping")
        return {"ok": True, "db": MONGO_DB, "server": "MongoDB Atlas", "connected": True}
    except Exception as e:
        _db_connected = False
        return JSONResponse(
            content={"ok": False, "error": str(e), "db": MONGO_DB},
            status_code=503
        )

@app.get("/api/debug")
async def debug_info():
    """Public debug endpoint — shows connection state without exposing credentials"""
    import re as _re
    safe_uri = _re.sub(r':(.*?)@', ':***@', MONGO_URI) if MONGO_URI else "NOT SET"
    return JSONResponse(content={
        "mongo_uri_set": bool(MONGO_URI),
        "mongo_uri_masked": safe_uri[:80] if MONGO_URI else "NOT SET",
        "mongo_db": MONGO_DB,
        "db_connected": _db_connected,
        "hint": "If db_connected=false, go to MongoDB Atlas → Network Access → Add 0.0.0.0/0"
    })

# ─────────────────────────────────────────────
#  SALES RECORDS
# ─────────────────────────────────────────────
@app.get("/api/sales")
async def get_sales(limit: int = Query(500, ge=1, le=2000), skip: int = Query(0, ge=0)):
    if not ensure_db():
        return JSONResponse(content=[], status_code=503)
    try:
        rows = list(col("sales_records").find({}, {"_id": 0})
                    .sort("SNo", DESCENDING).skip(skip).limit(limit))
        return JSONResponse(content=rows)
    except Exception as e:
        logger.error(f"get_sales error: {e}")
        return JSONResponse(content=[], status_code=500)

@app.post("/api/sales")
async def post_sales(request: Request):
    b = await request.json()
    sno = next_id("sales_records", "SNo")
    doc = {
        "SNo": sno,
        "Customer":           b.get("Customer", ""),
        "Category":           b.get("Category", ""),
        "ProductSize":        b.get("ProductSize", ""),
        "Size1":              b.get("Size1", ""),
        "Qty1":               b.get("Qty1", ""),
        "Size2":              b.get("Size2", ""),
        "Qty2":               b.get("Qty2", ""),
        "Size3":              b.get("Size3", ""),
        "Qty3":               b.get("Qty3", ""),
        "BillingType":        b.get("BillingType", ""),
        "JobName":            b.get("JobName", ""),
        "OrderDate":          b.get("OrderDate"),
        "TotalAmount":        to_float(b.get("TotalAmount")),
        "AdvanceAmt":         to_float(b.get("AdvanceAmt")),
        "AdvanceDate":        b.get("AdvanceDate"),
        "AdvanceMode":        b.get("AdvanceMode", ""),
        "BalanceSettledAmt":  to_float(b.get("BalanceSettledAmt")),
        "BalanceDate":        b.get("BalanceDate"),
        "BalanceMode":        b.get("BalanceMode", ""),
        "Balance2Amt":        to_float(b.get("Balance2Amt")),
        "Balance2Date":       b.get("Balance2Date"),
        "Balance2Mode":       b.get("Balance2Mode", ""),
        "Balance3Amt":        to_float(b.get("Balance3Amt")),
        "Balance3Date":       b.get("Balance3Date"),
        "Balance3Mode":       b.get("Balance3Mode", ""),
        "RemainingBalance":   to_float(b.get("RemainingBalance")),
        "ProductId":          to_int(b.get("ProductId")),
        "Rate1":              to_float(b.get("Rate1")),
        "Rate2":              to_float(b.get("Rate2")),
        "InvoiceNo":          b.get("InvoiceNo", ""),
        "UpdatedAt":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    col("sales_records").insert_one(doc)
    # Auto-add client
    if doc["Customer"]:
        col("rio_clients").update_one(
            {"ClientName": doc["Customer"]},
            {"$setOnInsert": {"Id": next_id("rio_clients"), "ClientName": doc["Customer"]}},
            upsert=True
        )
    # Ledger credits
    payments = [
        {"Amt": doc["AdvanceAmt"],        "Date": doc["AdvanceDate"],  "Mode": doc["AdvanceMode"]},
        {"Amt": doc["BalanceSettledAmt"], "Date": doc["BalanceDate"],  "Mode": doc["BalanceMode"]},
        {"Amt": doc["Balance2Amt"],       "Date": doc["Balance2Date"], "Mode": doc["Balance2Mode"]},
        {"Amt": doc["Balance3Amt"],       "Date": doc["Balance3Date"], "Mode": doc["Balance3Mode"]},
    ]
    set_sales_ledger_credits(sno, doc["Customer"], doc["JobName"], payments)
    return ok({"ok": True, "SNo": sno})

@app.put("/api/sales/{sno}")
async def put_sales(sno: int, request: Request):
    b = await request.json()
    update = {
        "Customer":           b.get("Customer", ""),
        "Category":           b.get("Category", ""),
        "ProductSize":        b.get("ProductSize", ""),
        "Size1":              b.get("Size1", ""),
        "Qty1":               b.get("Qty1", ""),
        "Size2":              b.get("Size2", ""),
        "Qty2":               b.get("Qty2", ""),
        "Size3":              b.get("Size3", ""),
        "Qty3":               b.get("Qty3", ""),
        "BillingType":        b.get("BillingType", ""),
        "JobName":            b.get("JobName", ""),
        "OrderDate":          b.get("OrderDate"),
        "TotalAmount":        to_float(b.get("TotalAmount")),
        "AdvanceAmt":         to_float(b.get("AdvanceAmt")),
        "AdvanceDate":        b.get("AdvanceDate"),
        "AdvanceMode":        b.get("AdvanceMode", ""),
        "BalanceSettledAmt":  to_float(b.get("BalanceSettledAmt")),
        "BalanceDate":        b.get("BalanceDate"),
        "BalanceMode":        b.get("BalanceMode", ""),
        "Balance2Amt":        to_float(b.get("Balance2Amt")),
        "Balance2Date":       b.get("Balance2Date"),
        "Balance2Mode":       b.get("Balance2Mode", ""),
        "Balance3Amt":        to_float(b.get("Balance3Amt")),
        "Balance3Date":       b.get("Balance3Date"),
        "Balance3Mode":       b.get("Balance3Mode", ""),
        "RemainingBalance":   to_float(b.get("RemainingBalance")),
        "ProductId":          to_int(b.get("ProductId")),
        "Rate1":              to_float(b.get("Rate1")),
        "Rate2":              to_float(b.get("Rate2")),
        "UpdatedAt":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    col("sales_records").update_one({"SNo": sno}, {"$set": update})
    payments = [
        {"Amt": update["AdvanceAmt"],        "Date": update["AdvanceDate"],  "Mode": update["AdvanceMode"]},
        {"Amt": update["BalanceSettledAmt"], "Date": update["BalanceDate"],  "Mode": update["BalanceMode"]},
        {"Amt": update["Balance2Amt"],       "Date": update["Balance2Date"], "Mode": update["Balance2Mode"]},
        {"Amt": update["Balance3Amt"],       "Date": update["Balance3Date"], "Mode": update["Balance3Mode"]},
    ]
    set_sales_ledger_credits(sno, update["Customer"], update["JobName"], payments)
    return ok()

@app.delete("/api/sales/{sno}")
async def delete_sales(sno: int):
    col("account_ledger").delete_many({"SalesRef": sno})
    col("sales_records").delete_one({"SNo": sno})
    return ok()

@app.post("/api/sales/{sno}/invoiceno")
async def patch_sales_invoiceno(sno: int, request: Request):
    b = await request.json()
    col("sales_records").update_one({"SNo": sno}, {"$set": {"InvoiceNo": b.get("InvoiceNo", "")}})
    return ok()

# ─────────────────────────────────────────────
#  EXPENSES
# ─────────────────────────────────────────────
@app.get("/api/expenses")
async def get_expenses():
    rows = list(col("daily_expenses").find({}, {"_id": 0}).sort([("ExpDate", DESCENDING), ("Id", DESCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/expenses")
async def post_expenses(request: Request):
    b = await request.json()
    new_id = next_id("daily_expenses")
    amt = to_float(b.get("Amount"), 0.0)
    doc = {
        "Id":          new_id,
        "ExpDate":     b.get("ExpDate"),
        "Category":    b.get("Category", ""),
        "SubCategory": b.get("SubCategory", ""),
        "PaymentMode": b.get("PaymentMode", ""),
        "Description": b.get("Description", ""),
        "Amount":      amt,
    }
    col("daily_expenses").insert_one(doc)
    # Auto-create ledger debit
    pm = (b.get("PaymentMode") or "").strip()
    acct_map = {"KVB MOM":"KVB MOM","KVB Mani":"KVB Mani","Indian Bank":"Indian Bank","Cash":"Cash Balance"}
    acct = acct_map.get(pm)
    if acct and new_id:
        exp_date = (b.get("ExpDate") or "").strip()
        fy = fy_from_date(exp_date)
        if fy:
            sub_cat = (b.get("SubCategory") or "").strip()
            desc_str = (b.get("Description") or "").strip()
            desc = f"Expense: {sub_cat} — {desc_str}" if desc_str else f"Expense: {sub_cat}"
            last = col("account_ledger").find_one(
                {"AccountName": acct, "FY": fy},
                sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)]
            )
            prev_bal = to_float(last["Balance"]) if last else 0.0
            new_bal  = prev_bal - amt
            led_id   = next_id("account_ledger")
            col("account_ledger").insert_one({
                "Id": led_id, "AccountName": acct, "EntryDate": exp_date,
                "Description": desc, "CreditAmt": 0, "DebitAmt": amt,
                "Balance": new_bal, "EntryType": "Expense", "FY": fy,
                "ExpenseRef": new_id, "SalesRef": None
            })
    return ok({"ok": True, "id": new_id})

@app.delete("/api/expenses/{exp_id}")
async def delete_expense(exp_id: int):
    col("account_ledger").delete_many({"ExpenseRef": exp_id})
    col("daily_expenses").delete_one({"Id": exp_id})
    return ok()

# ─────────────────────────────────────────────
#  NOTES
# ─────────────────────────────────────────────
@app.get("/api/notes")
async def get_notes(fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None)):
    query = {}
    if fr: query["NoteDate"] = {"$gte": fr}
    if to: query.setdefault("NoteDate", {})["$lte"] = to
    rows = list(col("notes").find(query, {"_id": 0}).sort([("NoteDate", DESCENDING), ("Id", DESCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/notes")
async def post_notes(request: Request):
    b = await request.json()
    new_id = next_id("notes")
    col("notes").insert_one({"Id": new_id, "NoteDate": b.get("NoteDate"), "NoteText": b.get("NoteText", "")})
    return ok()

@app.put("/api/notes/{note_id}")
async def put_notes(note_id: int, request: Request):
    b = await request.json()
    col("notes").update_one({"Id": note_id}, {"$set": {"NoteDate": b.get("NoteDate"), "NoteText": b.get("NoteText", "")}})
    return ok()

@app.delete("/api/notes/{note_id}")
async def delete_notes(note_id: int):
    col("notes").delete_one({"Id": note_id})
    return ok()

# ─────────────────────────────────────────────
#  FOLLOWUPS
# ─────────────────────────────────────────────
@app.get("/api/followups")
async def get_followups():
    rows = list(col("followups").find({}, {"_id": 0}).sort([("IsAddressed", ASCENDING), ("FollowupDate", ASCENDING), ("Id", ASCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/followups")
async def post_followups(request: Request):
    b = await request.json()
    new_id = next_id("followups")
    col("followups").insert_one({
        "Id": new_id,
        "FollowupDate": b.get("FollowupDate"),
        "Priority":     b.get("Priority", ""),
        "FollowupText": b.get("FollowupText", ""),
        "IsAddressed":  0,
    })
    return ok()

@app.put("/api/followups/{fid}/address")
async def address_followup(fid: int):
    col("followups").update_one({"Id": fid}, {"$set": {"IsAddressed": 1}})
    return ok()

@app.put("/api/followups/{fid}/reopen")
async def reopen_followup(fid: int, request: Request):
    b = await request.json()
    new_date = b.get("FollowupDate") or datetime.now().strftime("%Y-%m-%d")
    col("followups").update_one({"Id": fid}, {"$set": {"IsAddressed": 0, "FollowupDate": new_date}})
    return ok()

@app.put("/api/followups/{fid}")
async def put_followup(fid: int, request: Request):
    b = await request.json()
    col("followups").update_one({"Id": fid}, {"$set": {
        "FollowupDate": b.get("FollowupDate"),
        "Priority":     b.get("Priority", ""),
        "FollowupText": b.get("FollowupText", ""),
    }})
    return ok()

@app.delete("/api/followups/{fid}")
async def delete_followup(fid: int):
    col("followups").delete_one({"Id": fid})
    return ok()

# ─────────────────────────────────────────────
#  CLIENTS
# ─────────────────────────────────────────────
@app.get("/api/clients")
async def get_clients():
    rows = list(col("rio_clients").find({}, {"_id": 0, "ClientName": 1}).sort("ClientName", ASCENDING))
    return JSONResponse(content=[r["ClientName"] for r in rows if r.get("ClientName")])

@app.post("/api/clients")
async def post_clients(request: Request):
    b = await request.json()
    name = (b.get("ClientName") or "").strip()
    if name:
        col("rio_clients").update_one(
            {"ClientName": name},
            {"$setOnInsert": {"Id": next_id("rio_clients"), "ClientName": name}},
            upsert=True
        )
    return ok()

# ─────────────────────────────────────────────
#  CATEGORIES
# ─────────────────────────────────────────────
@app.delete("/api/clients/{client_name:path}")
async def delete_client(client_name: str):
    """Delete a client from the sales tracker clientsList."""
    if not ensure_db(): return JSONResponse(content={"error":"DB offline"}, status_code=503)
    name = client_name.strip()
    col("rio_clients").delete_one({"ClientName": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})
    return ok({"success": True, "deleted": name})


@app.get("/api/categories")
async def get_categories():
    rows = list(col("expense_categories").distinct("CategoryName"))
    return JSONResponse(content=sorted(rows))

@app.get("/api/categories/all")
async def get_categories_all():
    rows = list(col("expense_categories").find({}, {"_id": 0}).sort([("CategoryName", ASCENDING), ("SubCategoryName", ASCENDING)]))
    from collections import defaultdict
    mp = defaultdict(list)
    for r in rows:
        mp[r["CategoryName"]].append(r["SubCategoryName"])
    return JSONResponse(content=[{"category": k, "subcats": v} for k, v in mp.items()])

@app.get("/api/categories/subcats")
async def get_subcats(cat: str = Query("")):
    rows = list(col("expense_categories").find({"CategoryName": cat}, {"_id": 0, "SubCategoryName": 1}).sort("SubCategoryName", ASCENDING))
    subs = [r["SubCategoryName"] for r in rows]
    return JSONResponse(content=subs if subs else ["Other"])

@app.post("/api/categories")
async def post_category(request: Request):
    b = await request.json()
    cn = (b.get("CategoryName") or "").strip()
    sn = (b.get("SubCategoryName") or "").strip()
    if cn and sn:
        exists = col("expense_categories").find_one({"CategoryName": cn, "SubCategoryName": sn})
        if not exists:
            new_id = next_id("expense_categories")
            col("expense_categories").insert_one({"Id": new_id, "CategoryName": cn, "SubCategoryName": sn})
    return ok()

@app.post("/api/categories/sync")
async def sync_categories(request: Request):
    rows = await request.json()
    inserted = 0
    for row in (rows if isinstance(rows, list) else []):
        cn = (row.get("CategoryName") or "").strip()
        sn = (row.get("SubCategoryName") or "").strip()
        if cn and sn:
            exists = col("expense_categories").find_one({"CategoryName": cn, "SubCategoryName": sn})
            if not exists:
                new_id = next_id("expense_categories")
                col("expense_categories").insert_one({"Id": new_id, "CategoryName": cn, "SubCategoryName": sn})
                inserted += 1
    return ok({"ok": True, "inserted": inserted})

@app.delete("/api/categories/{cat_id}")
async def delete_category(cat_id: int):
    col("expense_categories").delete_one({"Id": cat_id})
    return ok()

# ─────────────────────────────────────────────
#  JOBS
# ─────────────────────────────────────────────
@app.get("/api/jobs")
async def get_jobs(fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None)):
    query = {}
    if fr: query["ConfirmedDate"] = {"$gte": fr}
    if to: query.setdefault("ConfirmedDate", {})["$lte"] = to
    rows = list(col("jobs").find(query, {"_id": 0}).sort("Id", DESCENDING))
    return JSONResponse(content=rows)

@app.post("/api/jobs")
async def post_jobs(request: Request):
    b = await request.json()
    new_id = next_id("jobs")
    # Auto-generate JobNo if not provided: J001, J002, ...
    job_no = (b.get("JobNo") or "").strip()
    if not job_no:
        pipeline = [{"$group": {"_id": None, "max": {"$max": "$Id"}}}]
        res = list(col("jobs").aggregate(pipeline))
        max_id = to_int(res[0]["max"]) if res else 0
        job_no = f"J{(max_id + 1):03d}"
    col("jobs").insert_one({
        "Id":           new_id,
        "JobNo":        job_no,
        "Customer":     b.get("Customer", ""),
        "JobName":      b.get("JobName", ""),
        "ConfirmedDate":b.get("ConfirmedDate"),
        "ProductSize":  b.get("ProductSize", ""),
        "Qty":          to_int(b.get("Qty")),
        "Status":       b.get("Status", ""),
        "DispatchDate": b.get("DispatchDate"),
    })
    return ok({"ok": True, "id": new_id, "jobNo": job_no})

@app.put("/api/jobs/{job_id}")
async def put_jobs(job_id: int, request: Request):
    b = await request.json()
    update = {
        "Customer":     b.get("Customer", ""),
        "JobName":      b.get("JobName", ""),
        "ConfirmedDate":b.get("ConfirmedDate"),
        "ProductSize":  b.get("ProductSize", ""),
        "Qty":          to_int(b.get("Qty")),
        "Status":       b.get("Status", ""),
        "DispatchDate": b.get("DispatchDate"),
    }
    # Only update JobNo if provided (don't overwrite existing)
    if b.get("JobNo"):
        update["JobNo"] = b.get("JobNo")
    col("jobs").update_one({"Id": job_id}, {"$set": update})
    return ok()

@app.delete("/api/jobs/{job_id}")
async def delete_jobs(job_id: int):
    col("jobs").delete_one({"Id": job_id})
    return ok()

# ─────────────────────────────────────────────
#  ACCOUNT BALANCES
# ─────────────────────────────────────────────
@app.get("/api/accountbalances")
async def get_acct_balances():
    rows = list(col("account_balances").find({}, {"_id": 0}).sort([("EntryDate", DESCENDING), ("Id", DESCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/accountbalances")
async def post_acct_balance(request: Request):
    b = await request.json()
    new_id = next_id("account_balances")
    col("account_balances").insert_one({
        "Id":          new_id,
        "AccountName": b.get("AccountName", ""),
        "EntryDate":   b.get("EntryDate"),
        "Balance":     to_float(b.get("Balance"), 0.0),
        "Notes":       b.get("Notes", ""),
    })
    return ok({"ok": True, "id": new_id})

@app.delete("/api/accountbalances/{ab_id}")
async def delete_acct_balance(ab_id: int):
    col("account_balances").delete_one({"Id": ab_id})
    return ok()

# ─────────────────────────────────────────────
#  LEDGER
# ─────────────────────────────────────────────
@app.get("/api/ledger/debug")
async def ledger_debug():
    total = col("account_ledger").count_documents({})
    rows = list(col("account_ledger").find({}, {"_id": 0}).sort("Id", DESCENDING).limit(20))
    return JSONResponse(content={"total": total, "rows": rows})

@app.get("/api/ledger/prev-closing")
async def ledger_prev_closing(fy: str = Query("")):
    if not fy:
        return JSONResponse(content=[])
    fy_year = int(fy.split("-")[0])
    prev_fy = f"{fy_year-1}-{str(fy_year)[-2:]}"
    result = []
    for acct in ["KVB MOM", "KVB Mani", "Indian Bank", "Cash Balance"]:
        last = col("account_ledger").find_one(
            {"AccountName": acct, "FY": prev_fy},
            sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)]
        )
        bal = to_float(last["Balance"]) if last else 0.0
        result.append({"AccountName": acct, "ClosingBalance": bal})
    return JSONResponse(content=result)

@app.get("/api/ledger/opening")
async def get_ledger_opening(fy: str = Query("")):
    if not fy:
        return JSONResponse(content=[])
    rows = list(col("account_opening_balances").find({"FY": fy}, {"_id": 0}))
    return JSONResponse(content=rows)

@app.post("/api/ledger/opening")
async def post_ledger_opening(request: Request):
    b = await request.json()
    fy = (b.get("FY") or "").strip()
    if not fy:
        return err("FY required")
    for acct in ["KVB MOM", "KVB Mani", "Indian Bank", "Cash Balance"]:
        val = to_float(b.get(acct), 0.0)
        exists = col("account_opening_balances").find_one({"AccountName": acct, "FY": fy})
        if exists:
            col("account_opening_balances").update_one(
                {"AccountName": acct, "FY": fy},
                {"$set": {"OpeningBal": val}}
            )
        else:
            col("account_opening_balances").insert_one({"AccountName": acct, "FY": fy, "OpeningBal": val})
    return ok()

@app.delete("/api/ledger/clear-opening")
async def clear_ledger_opening(fy: str = Query("")):
    if not fy:
        return ok({"ok": False})
    col("account_ledger").delete_many({"EntryType": "Opening", "FY": fy})
    col("account_opening_balances").delete_many({"FY": fy})
    return ok()

@app.get("/api/ledger")
async def get_ledger(
    account: Optional[str] = Query(None),
    fy: Optional[str] = Query(None),
    month: Optional[str] = Query(None)
):
    if not fy:
        return JSONResponse(content=[])
    query = {"FY": fy}
    if account: query["AccountName"] = account
    if month:   query["EntryDate"] = {"$regex": f"^{month}"}
    rows = list(col("account_ledger").find(query, {"_id": 0}).sort([("EntryDate", ASCENDING), ("Id", ASCENDING)]))
    return JSONResponse(content=rows)

@app.post("/api/ledger")
async def post_ledger(request: Request):
    b = await request.json()
    acct  = (b.get("AccountName") or "").strip()
    dt    = (b.get("EntryDate") or "").strip() or datetime.now().strftime("%Y-%m-%d")
    desc  = (b.get("Description") or "").strip()
    cr    = to_float(b.get("CreditAmt"), 0.0)
    dr    = to_float(b.get("DebitAmt"), 0.0)
    etype = (b.get("EntryType") or "Manual").strip()
    fy    = (b.get("FY") or "").strip()
    if not acct: return err("AccountName required")
    if not fy:   return err("FY required")
    if etype == "Opening":
        new_bal = cr - dr
    else:
        last = col("account_ledger").find_one(
            {"AccountName": acct, "FY": fy},
            sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)]
        )
        if last:
            prev = to_float(last["Balance"], 0.0)
        else:
            ob = col("account_opening_balances").find_one({"AccountName": acct, "FY": fy})
            prev = to_float(ob["OpeningBal"]) if ob else 0.0
        new_bal = prev + cr - dr
    new_id = next_id("account_ledger")
    col("account_ledger").insert_one({
        "Id": new_id, "AccountName": acct, "EntryDate": dt,
        "Description": desc, "CreditAmt": cr, "DebitAmt": dr,
        "Balance": new_bal, "EntryType": etype, "FY": fy,
        "ExpenseRef": None, "SalesRef": None
    })
    return ok({"ok": True, "balance": new_bal})

@app.delete("/api/ledger/reset")
async def ledger_reset():
    col("account_ledger").delete_many({})
    col("account_opening_balances").delete_many({})
    return ok()

@app.delete("/api/ledger/{led_id}")
async def delete_ledger_entry(led_id: int):
    col("account_ledger").delete_one({"Id": led_id})
    return ok()

@app.post("/api/ledger/migrate")
async def ledger_migrate(request: Request):
    b = await request.json()
    fy = (b.get("FY") or "").strip()
    if not fy: return err("FY required")
    fy_from, fy_to = fy_range(fy)
    exp_count = sales_count = skip_count = 0

    # Import expenses
    exp_rows = list(col("daily_expenses").find(
        {"ExpDate": {"$gte": fy_from, "$lte": fy_to}},
        {"_id": 0}
    ).sort([("ExpDate", ASCENDING), ("Id", ASCENDING)]))

    for row in exp_rows:
        exp_id = to_int(row.get("Id"))
        already = col("account_ledger").count_documents({"ExpenseRef": exp_id})
        if already > 0: skip_count += 1; continue
        pm = (row.get("PaymentMode") or "").strip()
        acct_map = {"KVB MOM":"KVB MOM","KVB Mani":"KVB Mani","Indian Bank":"Indian Bank","Cash":"Cash Balance"}
        acct = acct_map.get(pm)
        if not acct: skip_count += 1; continue
        exp_date = row.get("ExpDate", "")
        amt = to_float(row.get("Amount"), 0.0)
        sub_cat = (row.get("SubCategory") or "").strip()
        desc_str = (row.get("Description") or "").strip()
        desc = f"Expense: {sub_cat} — {desc_str}" if desc_str else f"Expense: {sub_cat}"
        last = col("account_ledger").find_one({"AccountName": acct, "FY": fy}, sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)])
        prev = to_float(last["Balance"]) if last else 0.0
        new_bal = prev - amt
        led_id = next_id("account_ledger")
        col("account_ledger").insert_one({
            "Id": led_id, "AccountName": acct, "EntryDate": exp_date,
            "Description": desc, "CreditAmt": 0, "DebitAmt": amt,
            "Balance": new_bal, "EntryType": "Expense", "FY": fy,
            "ExpenseRef": exp_id, "SalesRef": None
        })
        exp_count += 1

    # Import sales payments
    sales_rows = list(col("sales_records").find(
        {"OrderDate": {"$gte": fy_from, "$lte": fy_to}},
        {"_id": 0}
    ).sort([("OrderDate", ASCENDING), ("SNo", ASCENDING)]))

    acct_map = {"KVB MOM":"KVB MOM","KVB Mani":"KVB Mani","Indian Bank":"Indian Bank","Cash":"Cash Balance"}
    for row in sales_rows:
        sno = to_int(row.get("SNo"))
        already = col("account_ledger").count_documents({"SalesRef": sno})
        if already > 0: skip_count += 1; continue
        cust = (row.get("Customer") or "").strip()
        job  = (row.get("JobName") or "").strip()
        jn_str = f" — {job}" if job else ""
        desc = f"Sales: {cust}{jn_str}"
        payments = [
            {"Amt": row.get("AdvanceAmt"),        "Date": row.get("AdvanceDate"),  "Mode": row.get("AdvanceMode","")},
            {"Amt": row.get("BalanceSettledAmt"), "Date": row.get("BalanceDate"),  "Mode": row.get("BalanceMode","")},
            {"Amt": row.get("Balance2Amt"),       "Date": row.get("Balance2Date"), "Mode": row.get("Balance2Mode","")},
            {"Amt": row.get("Balance3Amt"),       "Date": row.get("Balance3Date"), "Mode": row.get("Balance3Mode","")},
        ]
        added = False
        for pay in payments:
            amt  = to_float(pay["Amt"])
            pdate = (pay["Date"] or "").strip()
            mode = (pay["Mode"] or "").strip()
            if not amt or amt <= 0 or not pdate: continue
            acct = acct_map.get(mode)
            if not acct: continue
            last = col("account_ledger").find_one({"AccountName": acct, "FY": fy}, sort=[("EntryDate", DESCENDING), ("Id", DESCENDING)])
            prev = to_float(last["Balance"]) if last else 0.0
            new_bal = prev + amt
            led_id = next_id("account_ledger")
            col("account_ledger").insert_one({
                "Id": led_id, "AccountName": acct, "EntryDate": pdate,
                "Description": desc, "CreditAmt": amt, "DebitAmt": 0,
                "Balance": new_bal, "EntryType": "Credit", "FY": fy,
                "ExpenseRef": None, "SalesRef": sno
            })
            added = True
            sales_count += 1
        if not added: skip_count += 1

    return ok({"ok": True, "expenseEntries": exp_count, "salesEntries": sales_count, "skipped": skip_count})

# ─────────────────────────────────────────────
#  BILLING — CUSTOMERS
# ─────────────────────────────────────────────
@app.get("/api/billing/status")
async def billing_status():
    cc = col("rio_clients").count_documents({})
    pc = col("products").count_documents({})
    ic = col("sales_invoices").count_documents({})
    return JSONResponse(content={"ready": True, "server": "MongoDB Atlas", "database": MONGO_DB, "version": "3.0", "customers": cc, "products": pc, "invoices": ic})

@app.get("/api/billing/customers")
async def billing_get_customers(q: Optional[str] = Query(None)):
    if q:
        query = {"$or": [
            {"ClientName": {"$regex": q, "$options": "i"}},
            {"Mobile": {"$regex": q, "$options": "i"}},
            {"GSTNo": {"$regex": q, "$options": "i"}},
        ]}
    else:
        query = {}
    rows = list(col("rio_clients").find(query, {"_id": 0}).sort("ClientName", ASCENDING))
    # Rename ClientName → Name for billing compatibility
    result = []
    for r in rows:
        result.append({
            "Id": r.get("Id"), "Name": r.get("ClientName",""),
            "BillToAddress": r.get("BillToAddress",""), "ShipToAddress": r.get("ShipToAddress",""),
            "State": r.get("State",""), "StateCode": r.get("StateCode",""),
            "Mobile": r.get("Mobile",""), "GSTNo": r.get("GSTNo",""),
            "Email": r.get("Email",""), "CustomerType": r.get("CustomerType",""),
        })
    return JSONResponse(content=result)

@app.post("/api/billing/customers")
async def billing_post_customer(request: Request):
    b = await request.json()
    name = (b.get("Name") or "").strip()
    if not name: return err("Name required")
    existing = col("rio_clients").find_one({"ClientName": name})
    update_doc = {
        "BillToAddress": b.get("BillToAddress",""), "ShipToAddress": b.get("ShipToAddress",""),
        "State": b.get("State",""), "StateCode": b.get("StateCode",""),
        "Mobile": b.get("Mobile",""), "GSTNo": b.get("GSTNo",""),
        "Email": b.get("Email",""), "CustomerType": b.get("CustomerType",""),
    }
    if existing:
        col("rio_clients").update_one({"ClientName": name}, {"$set": update_doc})
        return ok({"success": True, "id": existing["Id"]})
    else:
        new_id = next_id("rio_clients")
        col("rio_clients").insert_one({"Id": new_id, "ClientName": name, **update_doc})
        return ok({"success": True, "id": new_id})

@app.get("/api/billing/customers/byname")
async def billing_customer_byname(name: str = Query("")):
    if not name: return err("name required")
    r = col("rio_clients").find_one({"ClientName": name}, {"_id": 0})
    if not r:
        r = col("rio_clients").find_one({"ClientName": {"$regex": name, "$options": "i"}}, {"_id": 0})
    if not r: return err("Not found", 404)
    return JSONResponse(content={"Id": r.get("Id"), "Name": r.get("ClientName",""), **{k: r.get(k,"") for k in ["BillToAddress","ShipToAddress","State","StateCode","Mobile","GSTNo","Email","CustomerType"]}})

@app.get("/api/billing/customers/{cust_id}")
async def billing_get_customer(cust_id: int):
    r = col("rio_clients").find_one({"Id": cust_id}, {"_id": 0})
    if not r: return err("Not found", 404)
    return JSONResponse(content={"Id": r.get("Id"), "Name": r.get("ClientName",""), **{k: r.get(k,"") for k in ["BillToAddress","ShipToAddress","State","StateCode","Mobile","GSTNo","Email","CustomerType"]}})

@app.put("/api/billing/customers/{cust_id}")
async def billing_put_customer(cust_id: int, request: Request):
    b = await request.json()
    ship = (b.get("ShipToAddress") or "").strip() or (b.get("BillToAddress") or "").strip()
    col("rio_clients").update_one({"Id": cust_id}, {"$set": {
        "ClientName": b.get("Name",""), "BillToAddress": b.get("BillToAddress",""),
        "ShipToAddress": ship, "State": b.get("State",""), "StateCode": b.get("StateCode",""),
        "Mobile": b.get("Mobile",""), "GSTNo": b.get("GSTNo",""),
        "Email": b.get("Email",""), "CustomerType": b.get("CustomerType",""),
    }})
    return ok({"success": True})

@app.delete("/api/billing/customers/{cust_id}")
async def billing_delete_customer(cust_id: int):
    col("rio_clients").delete_one({"Id": cust_id})
    return ok({"success": True})

# ─────────────────────────────────────────────
#  BILLING — PRODUCTS
# ─────────────────────────────────────────────
@app.get("/api/billing/products/nextcode")
async def billing_nextcode():
    return JSONResponse(content={"code": next_product_code()})

@app.get("/api/billing/products")
async def billing_get_products(q: Optional[str] = Query(None)):
    if q:
        query = {"$or": [{"Name": {"$regex": q, "$options": "i"}}, {"Code": {"$regex": q, "$options": "i"}}]}
    else:
        query = {}
    rows = list(col("products").find(query, {"_id": 0}).sort("Code", ASCENDING))
    return JSONResponse(content=rows)

@app.post("/api/billing/products")
async def billing_post_product(request: Request):
    b = await request.json()
    name = (b.get("Name") or "").strip()
    if not name: return err("Name required")
    code = (b.get("Code") or "").strip() or next_product_code()
    new_id = next_id("products")
    col("products").insert_one({
        "Id": new_id, "Code": code, "Name": name,
        "PrintName": b.get("PrintName",""), "HSN": b.get("HSN",""),
        "Category": b.get("Category",""), "Unit": b.get("Unit","Nos"),
        "GSTRate": to_float(b.get("GSTRate"), 18.0),
    })
    return ok({"success": True, "id": new_id, "code": code})

@app.get("/api/billing/products/{prod_id}")
async def billing_get_product(prod_id: int):
    r = col("products").find_one({"Id": prod_id}, {"_id": 0})
    if not r: return err("Not found", 404)
    return JSONResponse(content=r)

@app.put("/api/billing/products/{prod_id}")
async def billing_put_product(prod_id: int, request: Request):
    b = await request.json()
    col("products").update_one({"Id": prod_id}, {"$set": {
        "Code": b.get("Code",""), "Name": b.get("Name",""),
        "PrintName": b.get("PrintName",""), "HSN": b.get("HSN",""),
        "Category": b.get("Category",""), "Unit": b.get("Unit","Nos"),
        "GSTRate": to_float(b.get("GSTRate"), 18.0),
    }})
    return ok({"success": True})

@app.delete("/api/billing/products/{prod_id}")
async def billing_delete_product(prod_id: int):
    col("products").delete_one({"Id": prod_id})
    return ok({"success": True})

# ─────────────────────────────────────────────
#  BILLING — INVOICE SEQUENCES
# ─────────────────────────────────────────────
@app.get("/api/billing/invoices/peek")
async def billing_invoice_peek(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"invoiceNo": next_invoice_no(type, fy)})

@app.get("/api/billing/invoices/next")
async def billing_invoice_next(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"invoiceNo": next_invoice_no(type, fy)})

@app.post("/api/billing/invoices/resetsequence")
async def billing_reset_sequence(type: str = Query("GST")):
    return ok({"success": True, "type": type})

# ─────────────────────────────────────────────
#  BILLING — INVOICES
# ─────────────────────────────────────────────
@app.get("/api/billing/invoices/byno")
async def billing_invoice_byno(invno: str = Query(""), fy: str = Query("")):
    if not invno: return err("invno required")
    query = {"InvoiceNo": invno}
    if fy:
        fy_from, fy_to = fy_range(fy)
        query["InvoiceDate"] = {"$gte": fy_from, "$lte": fy_to}
    inv = col("sales_invoices").find_one(query, {"_id": 0}, sort=[("Id", DESCENDING)])
    if not inv: return err("Not found", 404)
    inv_id = inv.get("Id")
    items = list(col("sales_items").find({"InvoiceId": inv_id}, {"_id": 0}).sort("SNo", ASCENDING))
    # Fetch customer details
    cust = {}
    if inv.get("CustomerId"):
        c = col("rio_clients").find_one({"Id": inv["CustomerId"]}, {"_id": 0})
        if c:
            cust = {"CustomerAddress": c.get("BillToAddress",""), "CustomerState": c.get("State",""),
                    "CustomerStateCode": c.get("StateCode",""), "CustomerMobile": c.get("Mobile",""),
                    "CustomerGST": c.get("GSTNo",""), "CustomerEmail": c.get("Email","")}
    # Format date
    try:
        inv["InvoiceDate"] = datetime.strptime(inv["InvoiceDate"][:10], "%Y-%m-%d").strftime("%d-%m-%Y")
    except: pass
    return JSONResponse(content={**inv, **cust, "Items": items})

@app.get("/api/billing/invoices")
async def billing_get_invoices(
    page: int = Query(1), pageSize: int = Query(50),
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None), q: Optional[str] = Query(None)
):
    pageSize = max(1, min(pageSize, 500))
    query = {}
    if fr or to:
        query["InvoiceDate"] = {}
        if fr: query["InvoiceDate"]["$gte"] = fr
        if to: query["InvoiceDate"]["$lte"] = to
    if type == "GST":    query["BillingType"] = {"$in": ["GST", "IGST"]}
    if type == "NONGST": query["BillingType"] = "NON-GST"
    if q: query["$or"] = [{"CustomerName": {"$regex": q, "$options": "i"}}, {"InvoiceNo": {"$regex": q, "$options": "i"}}]
    total = col("sales_invoices").count_documents(query)
    skip  = (page - 1) * pageSize
    rows  = list(col("sales_invoices").find(query, {"_id": 0, "Id":1,"InvoiceNo":1,"InvoiceDate":1,"CustomerName":1,"BillingType":1,"SubTotal":1,"CGST":1,"SGST":1,"IGST":1,"TotalAmount":1,"Counter":1,"PaymentTerms":1})
                .sort([("InvoiceDate", DESCENDING), ("Id", DESCENDING)]).skip(skip).limit(pageSize))
    return JSONResponse(content={"data": rows, "total": total, "page": page, "pageSize": pageSize})

@app.get("/api/billing/invoices/{inv_id}")
async def billing_get_invoice(inv_id: int):
    r = col("sales_invoices").find_one({"Id": inv_id}, {"_id": 0})
    if not r: return err("Invoice not found", 404)
    return JSONResponse(content=r)

@app.post("/api/billing/invoices")
async def billing_post_invoice(request: Request):
    b = await request.json()
    inv_no = (b.get("InvoiceNo") or "").strip()
    if not inv_no: return err("InvoiceNo required")
    raw_date = b.get("InvoiceDate", "")
    try:
        inv_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        inv_date = datetime.now().strftime("%Y-%m-%d")
    fy = fy_from_date(inv_date)
    new_id = next_id("sales_invoices")
    doc = {
        "Id": new_id, "Branch": "HO", "InvoiceNo": inv_no, "InvoiceDate": inv_date,
        "CustomerId": to_int(b.get("CustomerId"), 0),
        "CustomerName": b.get("CustomerName",""), "BillingType": b.get("BillingType",""),
        "PlaceOfSupply": b.get("PlaceOfSupply",""), "PlaceOfSupplyCode": b.get("PlaceOfSupplyCode",""),
        "SubTotal": to_float(b.get("SubTotal"),0), "CGST": to_float(b.get("CGST"),0),
        "SGST": to_float(b.get("SGST"),0), "IGST": to_float(b.get("IGST"),0),
        "TotalAmount": to_float(b.get("TotalAmount"),0),
        "Counter": b.get("Counter",""), "PaymentTerms": b.get("PaymentTerms",""), "FY": fy
    }
    col("sales_invoices").insert_one(doc)
    sno = 1
    for item in (b.get("Items") or []):
        if not item: continue
        qty = to_float(item.get("Qty"),0); rate = to_float(item.get("Rate"),0)
        tv  = to_float(item.get("TaxableValue"), qty*rate if qty and rate else 0)
        it  = to_float(item.get("Total"), tv)
        if not item.get("ProductName") and not tv: continue
        col("sales_items").insert_one({
            "InvoiceId": new_id, "SNo": sno,
            "ProductName": item.get("ProductName",""), "HSN": item.get("HSN",""),
            "Qty": qty, "Rate": rate, "TaxableValue": tv,
            "GSTRate": to_float(item.get("GSTRate"),0), "Total": it,
            "SizeNotes": item.get("SizeNotes","")
        })
        sno += 1
    return ok({"success": True, "id": new_id, "invoiceNo": inv_no})

@app.put("/api/billing/invoices/{inv_id}")
async def billing_put_invoice(inv_id: int, request: Request):
    b = await request.json()
    raw_date = b.get("InvoiceDate","")
    try: inv_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except: inv_date = datetime.now().strftime("%Y-%m-%d")
    col("sales_invoices").update_one({"Id": inv_id}, {"$set": {
        "InvoiceDate": inv_date, "CustomerId": to_int(b.get("CustomerId"),0),
        "CustomerName": b.get("CustomerName",""), "BillingType": b.get("BillingType",""),
        "PlaceOfSupply": b.get("PlaceOfSupply",""), "PlaceOfSupplyCode": b.get("PlaceOfSupplyCode",""),
        "SubTotal": to_float(b.get("SubTotal"),0), "CGST": to_float(b.get("CGST"),0),
        "SGST": to_float(b.get("SGST"),0), "IGST": to_float(b.get("IGST"),0),
        "TotalAmount": to_float(b.get("TotalAmount"),0),
        "Counter": b.get("Counter",""), "PaymentTerms": b.get("PaymentTerms",""),
    }})
    col("sales_items").delete_many({"InvoiceId": inv_id})
    sno = 1
    for item in (b.get("Items") or []):
        if not item: continue
        qty = to_float(item.get("Qty"),0); rate = to_float(item.get("Rate"),0)
        tv  = to_float(item.get("TaxableValue"), qty*rate if qty and rate else 0)
        it  = to_float(item.get("Total"), tv)
        if not item.get("ProductName") and not tv: continue
        col("sales_items").insert_one({
            "InvoiceId": inv_id, "SNo": sno,
            "ProductName": item.get("ProductName",""), "HSN": item.get("HSN",""),
            "Qty": qty, "Rate": rate, "TaxableValue": tv,
            "GSTRate": to_float(item.get("GSTRate"),0), "Total": it,
            "SizeNotes": item.get("SizeNotes","")
        })
        sno += 1
    inv_no = col("sales_invoices").find_one({"Id": inv_id}, {"InvoiceNo": 1})
    return ok({"success": True, "id": inv_id, "invoiceNo": inv_no.get("InvoiceNo","") if inv_no else ""})

@app.delete("/api/billing/invoices/{inv_id}")
async def billing_delete_invoice(inv_id: int):
    inv = col("sales_invoices").find_one({"Id": inv_id}, {"InvoiceNo": 1})
    if not inv: return err("Invoice not found", 404)
    inv_no = inv.get("InvoiceNo","")
    # Only allow deleting the most recent invoice in its series
    if inv_no.startswith("RN"):
        latest = col("sales_invoices").find_one({"InvoiceNo": {"$regex": r"^RN"}}, sort=[("Id", DESCENDING)])
    else:
        latest = col("sales_invoices").find_one({"InvoiceNo": {"$regex": r"^R\d"}}, sort=[("Id", DESCENDING)])
    if not latest or latest.get("Id") != inv_id:
        return err("Only the most recent invoice in this series can be deleted.", 403)
    col("sales_items").delete_many({"InvoiceId": inv_id})
    col("sales_invoices").delete_one({"Id": inv_id})
    col("sales_records").update_many({"InvoiceNo": inv_no}, {"$set": {"InvoiceNo": ""}})
    return ok({"success": True, "invoiceNo": inv_no})

# ─────────────────────────────────────────────
#  BILLING — QUOTATIONS
# ─────────────────────────────────────────────
@app.get("/api/billing/quotations/peek")
async def billing_quotation_peek(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"quotationNo": next_quotation_no(type, fy)})

@app.get("/api/billing/quotations/next")
async def billing_quotation_next(type: str = Query("GST"), fy: str = Query("")):
    if not fy: fy = current_fy()
    return JSONResponse(content={"quotationNo": next_quotation_no(type, fy)})

@app.get("/api/billing/quotations/byno")
async def billing_quotation_byno(qno: str = Query("")):
    if not qno: return err("qno required")
    q = col("quotations").find_one({"QuotationNo": qno}, {"_id": 0})
    if not q: return err("Not found", 404)
    q_id = q.get("Id")
    items = list(col("quotation_items").find({"QuotationId": q_id}, {"_id": 0}).sort("SNo", ASCENDING))
    try:
        q["QuotationDate"] = datetime.strptime(q["QuotationDate"][:10], "%Y-%m-%d").strftime("%d-%m-%Y")
    except: pass
    try:
        q["ValidTill"] = datetime.strptime(q["ValidTill"][:10], "%Y-%m-%d").strftime("%d-%m-%Y")
    except: pass
    return JSONResponse(content={**q, "Items": items})

@app.get("/api/billing/quotations")
async def billing_get_quotations(
    page: int = Query(1), pageSize: int = Query(50),
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None)
):
    pageSize = max(1, min(pageSize, 500))
    query = {}
    if fr or to:
        query["QuotationDate"] = {}
        if fr: query["QuotationDate"]["$gte"] = fr
        if to: query["QuotationDate"]["$lte"] = to
    if type == "GST":    query["BillingType"] = {"$in": ["GST","IGST"]}
    if type == "NONGST": query["BillingType"] = "NON-GST"
    total = col("quotations").count_documents(query)
    skip  = (page - 1) * pageSize
    rows  = list(col("quotations").find(query, {"_id": 0})
                .sort([("QuotationDate", DESCENDING), ("Id", DESCENDING)]).skip(skip).limit(pageSize))
    return JSONResponse(content={"data": rows, "total": total, "page": page, "pageSize": pageSize})

@app.get("/api/billing/quotations/{quot_id}")
async def billing_get_quotation(quot_id: int):
    q = col("quotations").find_one({"Id": quot_id}, {"_id": 0})
    if not q: return err("Quotation not found", 404)
    items = list(col("quotation_items").find({"QuotationId": quot_id}, {"_id": 0}).sort("SNo", ASCENDING))
    return JSONResponse(content={**q, "Items": items})

@app.post("/api/billing/quotations")
async def billing_post_quotation(request: Request):
    b = await request.json()
    q_no = (b.get("QuotationNo") or "").strip()
    if not q_no: return err("QuotationNo required")
    try: q_date = datetime.strptime(b.get("QuotationDate","")[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except: q_date = datetime.now().strftime("%Y-%m-%d")
    try: vt = datetime.strptime(b.get("ValidTill","")[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except: vt = q_date
    new_id = next_id("quotations")
    doc = {
        "Id": new_id, "QuotationNo": q_no, "QuotationDate": q_date,
        "CustomerId": to_int(b.get("CustomerId"),0),
        "CustomerName": b.get("CustomerName",""), "BillingType": b.get("BillingType",""),
        "PlaceOfSupply": b.get("PlaceOfSupply",""), "PlaceOfSupplyCode": b.get("PlaceOfSupplyCode",""),
        "SubTotal": to_float(b.get("SubTotal"),0), "CGST": to_float(b.get("CGST"),0),
        "SGST": to_float(b.get("SGST"),0), "IGST": to_float(b.get("IGST"),0),
        "TotalAmount": to_float(b.get("TotalAmount"),0),
        "PaymentTerms": b.get("PaymentTerms",""), "ValidTill": vt
    }
    col("quotations").insert_one(doc)
    sno = 1
    for item in (b.get("Items") or []):
        if not item: continue
        qty = to_float(item.get("Qty"),0); rate = to_float(item.get("Rate"),0)
        tv  = to_float(item.get("TaxableValue"), qty*rate if qty and rate else 0)
        it  = to_float(item.get("Total"), tv)
        if not item.get("ProductName") and not tv: continue
        col("quotation_items").insert_one({
            "QuotationId": new_id, "SNo": sno,
            "ProductName": item.get("ProductName",""), "HSN": item.get("HSN",""),
            "Qty": qty, "Rate": rate, "TaxableValue": tv,
            "GSTRate": to_float(item.get("GSTRate"),0), "Total": it,
            "SizeNotes": item.get("SizeNotes","")
        })
        sno += 1
    return ok({"success": True, "id": new_id, "quotationNo": q_no})

@app.delete("/api/billing/quotations/{quot_id}")
async def billing_delete_quotation(quot_id: int):
    latest = col("quotations").find_one({}, sort=[("Id", DESCENDING)])
    if not latest or latest.get("Id") != quot_id:
        return err("Only the most recently saved quotation can be deleted.", 403)
    col("quotation_items").delete_many({"QuotationId": quot_id})
    col("quotations").delete_one({"Id": quot_id})
    return ok({"success": True})

# ─────────────────────────────────────────────
#  BILLING — REPORTS
# ─────────────────────────────────────────────
@app.get("/api/billing/reports/sales")
async def billing_reports_sales(
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None)
):
    query = {}
    if fr or to:
        query["InvoiceDate"] = {}
        if fr: query["InvoiceDate"]["$gte"] = fr
        if to: query["InvoiceDate"]["$lte"] = to
    if type == "GST":    query["BillingType"] = {"$in": ["GST","IGST"]}
    if type == "NONGST": query["BillingType"] = "NON-GST"
    rows = list(col("sales_invoices").find(query, {"_id":0,"InvoiceNo":1,"InvoiceDate":1,"CustomerName":1,"BillingType":1,"SubTotal":1,"CGST":1,"SGST":1,"IGST":1,"TotalAmount":1})
                .sort([("InvoiceDate", DESCENDING), ("Id", DESCENDING)]))
    totals = {"SubTotal":0,"CGST":0,"SGST":0,"IGST":0,"TotalAmount":0}
    for r in rows:
        for k in totals:
            totals[k] += to_float(r.get(k),0)
    return JSONResponse(content={"data": rows, "count": len(rows), "totals": totals})

# ─────────────────────────────────────────────
#  BILLING — BACKUP (stub — data is in MongoDB)
# ─────────────────────────────────────────────
@app.post("/api/billing/backup")
async def billing_backup():
    return ok({"success": True, "message": "Data is stored in MongoDB Atlas — no local backup needed. Use MongoDB Atlas backup features.", "recentBackups": []})

@app.get("/api/billing/backups")
async def billing_backups():
    return JSONResponse(content=[])

@app.post("/api/billing/reset-sequences")
async def billing_reset_sequences():
    return ok({"message": "Sequences are auto-calculated from existing records in MongoDB.", "invoiceCount": 0, "quotationCount": 0})

# ─────────────────────────────────────────────
#  REPORTS (non-billing)
# ─────────────────────────────────────────────
@app.get("/api/reports/sales")
async def reports_sales(
    fr: Optional[str] = Query(None, alias="from"), to: Optional[str] = Query(None),
    type: Optional[str] = Query(None)
):
    return await billing_reports_sales(fr=fr, to=to, type=type)

# ─────────────────────────────────────────────
#  CLIENT-SIDE LOGGING  →  uses main logger
# ─────────────────────────────────────────────
# Reuse the top-level logger (already configured above with file + stdout)
_log = logging.getLogger("rio_api")
# LOG_FILE exposed for /api/log/tail endpoint
LOG_FILE = _LOG_FILE  # None on Render, Path on Windows

class LogEntry(BaseModel):
    level:   str = "INFO"
    user:    str = "unknown"
    action:  str = ""
    detail:  str = ""
    page:    str = ""
    ts:      str = ""

@app.get("/api/log/where")
async def log_where():
    """Shows where the log file is (or tells you it's on Render stdout)."""
    import platform as _p
    return {
        "platform": _p.system(),
        "log_file": str(LOG_FILE) if LOG_FILE else None,
        "log_exists": LOG_FILE.exists() if LOG_FILE else False,
        "note": (
            f"Log file at: {LOG_FILE}" if LOG_FILE and LOG_FILE.exists()
            else "Running on Render/Linux — logs go to Render dashboard Logs tab, not a file."
        )
    }

@app.get("/api/log/tail")
async def log_tail(n: int = 100):
    """Return last n lines of the log file (Windows only; Render uses stdout)."""
    if not LOG_FILE or not LOG_FILE.exists():
        return {"lines": [], "note": "Log file not available on this platform. On Render, check the Logs tab in the dashboard."}
    try:
        lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
        return {"lines": lines[-n:], "total": len(lines), "file": str(LOG_FILE)}
    except Exception as e:
        return {"lines": [], "error": str(e)}


# ─────────────────────────────────────────────
#  ATTENDANCE — matches PS1 v2.1 structure
#  Collection: att_records, att_staff
# ─────────────────────────────────────────────
@app.get("/api/attendance/ping")
async def att_ping():
    if not ensure_db(): return JSONResponse(content={"ok": False}, status_code=503)
    return {"ok": True}

@app.get("/api/attendance/staff")
async def get_att_staff():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    try:
        rows = list(col("att_staff").find({}, {"_id": 0}).sort("name", ASCENDING))
        return JSONResponse(content=rows)
    except Exception as e:
        return JSONResponse(content=[], status_code=500)

@app.post("/api/attendance/staff")
async def post_att_staff(request: Request):
    staff = await request.json()
    if not isinstance(staff, list): return err("Expected array")
    if not ensure_db(): return err("DB offline")
    try:
        col("att_staff").delete_many({})
        if staff:
            col("att_staff").insert_many(
                [{k:v for k,v in s.items()} for s in staff], ordered=False
            )
    except Exception as e:
        return err(str(e))
    return ok({"success": True})

@app.get("/api/attendance")
async def get_attendance(date: Optional[str] = Query(None), month: Optional[str] = Query(None)):
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    try:
        query = {}
        if date:  query["date"] = date
        elif month: query["date"] = {"$regex": f"^{month}"}
        rows = list(col("att_records").find(query, {"_id": 0}))
        return JSONResponse(content=rows)
    except Exception as e:
        return JSONResponse(content=[], status_code=500)

@app.post("/api/attendance/upsert")
async def att_upsert(request: Request):
    rec = await request.json()
    if not ensure_db(): return err("DB offline")
    try:
        rec_id = rec.get("id")
        name   = rec.get("name","").strip()
        date   = rec.get("date","").strip()
        if not name or not date: return err("name and date required")
        # Remove MongoDB _id if present
        rec.pop("_id", None)
        col("att_records").replace_one({"name": name, "date": date}, rec, upsert=True)
        return ok({"success": True})
    except Exception as e:
        return err(str(e))

@app.get("/api/attendance/all")
async def att_get_all():
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    try:
        rows = list(col("att_records").find({}, {"_id": 0}).sort([("date", ASCENDING), ("name", ASCENDING)]))
        return JSONResponse(content=rows)
    except Exception as e:
        return JSONResponse(content=[], status_code=500)

@app.get("/api/attendance/search")
async def att_search(
    fr:   Optional[str] = Query(None, alias="from"),
    to:   Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    jobType: Optional[str] = Query(None)
):
    if not ensure_db(): return JSONResponse(content=[], status_code=503)
    try:
        query = {}
        if fr or to:
            query["date"] = {}
            if fr: query["date"]["$gte"] = fr
            if to: query["date"]["$lte"] = to
        if name:    query["name"]    = name
        if jobType: query["jobType"] = jobType
        rows = list(col("att_records").find(query, {"_id": 0}).sort([("date", ASCENDING), ("name", ASCENDING)]))
        return JSONResponse(content=rows)
    except Exception as e:
        return JSONResponse(content=[], status_code=500)

@app.delete("/api/attendance/delete")
async def att_delete(
    fr:   Optional[str] = Query(None, alias="from"),
    to:   Optional[str] = Query(None),
    name: Optional[str] = Query(None)
):
    if not ensure_db(): return err("DB offline")
    try:
        query = {}
        if fr or to:
            query["date"] = {}
            if fr: query["date"]["$gte"] = fr
            if to: query["date"]["$lte"] = to
        if name: query["name"] = name
        result = col("att_records").delete_many(query)
        return ok({"deleted": result.deleted_count})
    except Exception as e:
        return err(str(e))

@app.post("/api/attendance")
async def post_attendance(request: Request):
    b = await request.json()
    date    = b.get("date", "")
    records = b.get("records", [])
    if not date or not records: return err("date and records required")
    if not ensure_db(): return err("DB offline")
    try:
        for rec in records:
            rec["date"] = date
            col("att_records").replace_one(
                {"staffId": rec.get("staffId"), "date": date},
                rec, upsert=True
            )
        return ok({"success": True, "saved": len(records)})
    except Exception as e:
        return err(str(e))


# ─────────────────────────────────────────────
#  AUTH — Login / Users
# ─────────────────────────────────────────────
import secrets

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())

def ensure_default_users():
    """Always ensure admin user exists — upsert so it survives even if other users exist."""
    existing = col("rio_users").find_one({"username": "admin"})
    if not existing:
        col("rio_users").insert_one(
            {"username": "admin", "password": hash_password("rio@admin"), "role": "admin", "name": "Administrator"}
        )
        logger.info("✓ Admin user created: username=admin password=rio@admin")
    else:
        # Ensure role is admin (fix any corruption)
        if existing.get("role") != "admin":
            col("rio_users").update_one({"username": "admin"}, {"$set": {"role": "admin"}})
            logger.info("✓ Admin role corrected")


# ─────────────────────────────────────────────
#  AUTH — Emergency admin reset (upsert admin user)
# ─────────────────────────────────────────────

@app.post("/api/auth/login")
async def login(request: Request):
    if not ensure_db():
        return JSONResponse(content={"ok": False, "error": "Database not connected. Please try again in a moment."}, status_code=503)
    b = await request.json()
    username = (b.get("username") or "").strip().lower()
    password = (b.get("password") or "").strip()
    if not username or not password:
        return err("Username and password required", 400)
    user = col("rio_users").find_one({"username": username}, {"_id": 0})
    if not user:
        return err("Invalid username or password", 401)
    if not verify_password(password, user["password"]):
        return err("Invalid username or password", 401)
    valid_roles = {"admin", "partner", "auditor", "expense", "invoice", "guest"}
    role = user.get("role", "guest")
    if role not in valid_roles:
        role = "guest"
    # Generate a session token and store it so protected endpoints can verify
    token = secrets.token_hex(32)
    col("rio_users").update_one(
        {"username": username},
        {"$set": {"session_token": token}}
    )
    return JSONResponse(content={
        "ok": True,
        "username": user["username"],
        "name": user.get("name", username),
        "role": role,
        "token": token
    })

@app.get("/api/auth/users")
async def get_users():
    if not ensure_db():
        return JSONResponse(content={"error": "Database not connected"}, status_code=503)
    users = list(col("rio_users").find({}, {"_id": 0, "password": 0, "session_token": 0}))
    return JSONResponse(content=users)

@app.post("/api/auth/users")
async def create_user(request: Request):
    b = await request.json()
    username = (b.get("username") or "").strip().lower()
    password = (b.get("password") or "").strip()
    role     = (b.get("role") or "expense").strip()
    name     = (b.get("name") or username).strip()
    if not username or not password:
        return err("Username and password required")
    valid_roles = {"admin", "partner", "auditor", "expense", "invoice", "guest"}
    if role not in valid_roles:
        return err(f"Invalid role. Must be one of: {', '.join(sorted(valid_roles))}")
    if col("rio_users").find_one({"username": username}):
        return err("Username already exists")
    col("rio_users").insert_one({
        "username": username,
        "password": hash_password(password),
        "role": role,
        "name": name
    })
    return ok({"ok": True})

@app.put("/api/auth/users/{username}")
async def update_user(username: str, request: Request):
    b = await request.json()
    update = {}
    if b.get("password"): update["password"] = hash_password(b["password"])
    if b.get("role"):     update["role"]     = b["role"]
    if b.get("name"):     update["name"]     = b["name"]
    col("rio_users").update_one({"username": username}, {"$set": update})
    return ok()

@app.delete("/api/auth/users/{username}")
async def delete_user(username: str):
    if username == "admin":
        return err("Cannot delete admin user")
    col("rio_users").delete_one({"username": username})
    return ok()
