"""
rio_api.py — RIO PRINT MEDIA Sales Tracker v3.0
FastAPI backend replacing the PowerShell script.
Data stored in MongoDB Atlas.

Run locally:
    uvicorn rio_api:app --host 0.0.0.0 --port 8765 --reload

Deploy on Render.com:
    Start command: uvicorn rio_api:app --host 0.0.0.0 --port $PORT
"""

import os, re, bcrypt, sys
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional, Any

from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from bson import ObjectId
from dotenv import load_dotenv

# Use logging so uvicorn captures and displays output properly
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rio_api")

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
MONGO_URI  = os.environ.get("MONGO_URI", "")
MONGO_DB   = os.environ.get("MONGO_DB",  "RioPrintMedia")
HTML_FILE  = os.environ.get("HTML_FILE", "Rio_Sales_Tracker_v3_0.html")

# ── Startup diagnostics ──
logger.info("=" * 60)
logger.info("RIO API STARTING UP")
logger.info(f"MONGO_DB  = {MONGO_DB}")
logger.info(f"MONGO_URI = {'SET (' + MONGO_URI[:20] + '...)' if MONGO_URI else 'NOT SET — check Render Environment Variables!'}")
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
        print("✗ MONGO_URI is empty — set it in Render → Environment Variables", flush=True)
        _db_connected = False
        return False
    try:
        logger.info(f"Connecting to MongoDB: {MONGO_DB} ...")
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=20000,
            connectTimeoutMS=20000,
            socketTimeoutMS=30000
        )
        _client.admin.command("ping")
        _db = _client[MONGO_DB]
        _db_connected = True
        logger.info(f"Connected to MongoDB: {MONGO_DB}")
        return True
    except Exception as e:
        _db_connected = False
        logger.error(f"MongoDB connection FAILED: {e}")
        logger.error(f"URI starts with: {MONGO_URI[:30]}...")
        return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # DO NOT raise — keep server alive even if DB is temporarily down.
    # Render cold starts can be slow; server retries on first real request.
    connected = _connect_mongo()
    if connected:
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
CLOUD_URL = "https://rio-print-media.onrender.com"

INJECT_CSS = """<style>
#rio-login-overlay{position:fixed;top:0;left:0;width:100vw;height:100vh;background:linear-gradient(135deg,#0e1220 0%,#1a237e 100%);display:none;align-items:center;justify-content:center;z-index:99999;flex-direction:column;}
#rio-login-box{background:white;border-radius:18px;padding:36px 32px 28px;width:90%;max-width:380px;box-shadow:0 20px 60px rgba(0,0,0,0.5);}
#rio-login-box h2{margin:0 0 4px;font-family:'Exo 2',sans-serif;font-size:1.4rem;color:#1a237e;text-align:center;}
#rio-login-box p{margin:0 0 24px;font-size:0.82rem;color:#888;text-align:center;font-family:'Nunito',sans-serif;}
.rio-login-field{width:100%;margin-bottom:14px;box-sizing:border-box;border:1.5px solid #e0e0e0;border-radius:10px;padding:12px 14px;font-size:1rem;font-family:'Nunito',sans-serif;outline:none;transition:border 0.2s;}
.rio-login-field:focus{border-color:#3949ab;}
#rio-login-btn{width:100%;padding:13px;background:linear-gradient(90deg,#1a237e,#3949ab);color:white;border:none;border-radius:10px;font-size:1rem;font-family:'Exo 2',sans-serif;font-weight:700;cursor:pointer;margin-top:4px;letter-spacing:0.5px;}
#rio-login-err{color:#c62828;font-size:0.82rem;text-align:center;margin-top:10px;font-family:'Nunito',sans-serif;min-height:18px;}
#rio-login-logo{font-family:'Exo 2',sans-serif;font-size:1.1rem;font-weight:900;color:white;margin-bottom:22px;letter-spacing:2px;text-align:center;opacity:0.9;}
#rio-logout-btn{position:fixed;top:10px;right:12px;z-index:9000;background:#c62828;color:white;border:none;border-radius:8px;padding:7px 16px;font-family:'Exo 2',sans-serif;font-weight:700;font-size:0.78rem;cursor:pointer;box-shadow:0 2px 8px rgba(0,0,0,0.3);}
body.role-expense #nav{display:none!important;}body.role-expense .panel:not(#panel-expense){display:none!important;}body.role-expense #panel-expense{display:block!important;}body.role-expense #content{margin-left:0!important;padding:12px!important;}
body.role-invoice #nav{display:none!important;}body.role-invoice .panel:not(#panel-rio-invoice):not(#panel-rio-quotation):not(#panel-rio-invmgr):not(#panel-rio-customers):not(#panel-rio-products):not(#panel-rio-reports){display:none!important;}body.role-invoice #panel-rio-invoice{display:flex!important;}body.role-invoice #content{margin-left:0!important;padding:12px!important;}
body.role-guest .form-section,body.role-guest .action-bar,body.role-guest .btn-action,body.role-guest td button,body.role-guest .edit-btn,body.role-guest #s-edit-bar{display:none!important;}
#usermgmt-table{width:100%;border-collapse:collapse;font-family:'Nunito',sans-serif;font-size:0.85rem;}
#usermgmt-table th{background:#1a237e;color:white;padding:10px 12px;text-align:left;font-family:'Exo 2',sans-serif;font-size:0.78rem;letter-spacing:0.5px;}
#usermgmt-table td{padding:9px 12px;border-bottom:1px solid #e8eaf6;vertical-align:middle;}
.role-badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:0.72rem;font-family:'Exo 2',sans-serif;font-weight:700;letter-spacing:0.5px;}
.role-admin{background:#e8eaf6;color:#1a237e;}.role-partner{background:#e8f5e9;color:#1b5e20;}.role-auditor{background:#fff3e0;color:#e65100;}.role-expense{background:#fce4ec;color:#880e4f;}.role-invoice{background:#e0f2f1;color:#004d40;}.role-guest{background:#f5f5f5;color:#616161;}
</style>"""

LOGIN_HTML = """
<div id="rio-login-overlay" style="display:none">
  <div id="rio-login-logo">&#128424; RIO PRINT MEDIA</div>
  <div id="rio-login-box">
    <h2>Welcome Back</h2><p>Sign in to continue</p>
    <input class="rio-login-field" id="rio-uname" type="text" placeholder="Username" autocomplete="username" onkeydown="if(event.key==='Enter')document.getElementById('rio-pwd').focus()">
    <input class="rio-login-field" id="rio-pwd" type="password" placeholder="Password" autocomplete="current-password" onkeydown="if(event.key==='Enter')rioDoLogin()">
    <button id="rio-login-btn" onclick="rioDoLogin()">Sign In</button>
    <div id="rio-login-err"></div>
  </div>
</div>
<button id="rio-logout-btn" style="display:none" onclick="rioLogout()">&#9211; Logout</button>
"""

AUTH_JS = """<script>
var _rioUser=null;
function rioAuthHeaders(){var u=window._rioUser;var h={'Content-Type':'application/json'};if(u&&u.token)h['Authorization']='Bearer '+u.token;return h;}
async function rioDoLogin(){
  var uname=document.getElementById('rio-uname').value.trim();
  var pwd=document.getElementById('rio-pwd').value.trim();
  var errEl=document.getElementById('rio-login-err');
  var btn=document.getElementById('rio-login-btn');
  if(!uname||!pwd){errEl.textContent='Enter username and password';return;}
  btn.textContent='Signing in...';btn.disabled=true;
  try{
    var r=await fetch('""" + CLOUD_URL + """/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:uname,password:pwd})});
    var d=await r.json();
    if(!r.ok||!d.ok){errEl.textContent=d.error||'Invalid username or password';btn.textContent='Sign In';btn.disabled=false;return;}
    _rioUser=d;window._rioUser=d;
    sessionStorage.setItem('rio_user',JSON.stringify(d));
    rioApplyRole(d);
  }catch(e){errEl.textContent='Connection error — try again';btn.textContent='Sign In';btn.disabled=false;}
}
function rioApplyRole(user){
  var overlay=document.getElementById('rio-login-overlay');if(overlay)overlay.style.display='none';
  var lb=document.getElementById('rio-logout-btn');if(lb){lb.style.display='block';lb.textContent=(user.name||user.username)+'  \u23fb';}
  var role=user.role;
  document.body.classList.remove('role-expense','role-invoice','role-guest','role-admin','role-partner','role-auditor');
  document.body.classList.add('role-'+role);
  if(role==='expense'){document.querySelectorAll('.panel').forEach(function(p){p.style.display='none';});var ep=document.getElementById('panel-expense');if(ep){ep.style.display='block';ep.classList.add('active');}if(typeof loadExpenses==='function')loadExpenses();}
  else if(role==='invoice'){document.querySelectorAll('.panel').forEach(function(p){p.style.display='none';});var ip=document.getElementById('panel-rio-invoice');if(ip){ip.style.display='flex';ip.classList.add('active');}if(typeof invNewInvoice==='function')invNewInvoice();if(typeof loadInvRegister==='function')loadInvRegister();}
  else{loadAll();loadJobs();setInterval(loadAll,60000);}
  var umgmt=document.getElementById('rio-usermgmt-section');if(umgmt)umgmt.style.display=(role==='admin')?'block':'none';
}
function rioLogout(){sessionStorage.removeItem('rio_user');_rioUser=null;window._rioUser=null;location.reload();}
function rioAuthInit(){
  try{var saved=sessionStorage.getItem('rio_user');if(saved){var user=JSON.parse(saved);if(user&&user.username&&user.role){_rioUser=user;window._rioUser=user;rioApplyRole(user);return;}}}catch(e){}
  var overlay=document.getElementById('rio-login-overlay');if(overlay)overlay.style.display='flex';
  setTimeout(function(){var u=document.getElementById('rio-uname');if(u)u.focus();},200);
}
</script>"""

def _apply_patches(html: str) -> str:
    """Apply all cloud patches to raw HTML before serving."""
    import re

    # Always force cloud API URL — replace any existing detectPort
    cloud_detect = (
        "async function detectPort() {\n"
        "  API='" + CLOUD_URL + "/api';window.API=API;\n"
        "  window._BILLING_BASE='" + CLOUD_URL + "';\n"
        "  try{const r=await fetch(API+'/ping',{cache:'no-store'});if(r.ok)return true;}catch(e){}\n"
        "  return false;\n"
        "}"
    )
    # Replace detectPort using start marker
    if 'async function detectPort' in html:
        start = html.find('async function detectPort')
        # Find closing } by counting braces
        depth = 0
        end = start
        found_open = False
        for i in range(start, len(html)):
            if html[i] == '{':
                depth += 1
                found_open = True
            elif html[i] == '}':
                depth -= 1
                if found_open and depth == 0:
                    end = i + 1
                    break
        html = html[:start] + cloud_detect + html[end:]

    # Always replace BILLING_API
    html = re.sub(
        r'BILLING_API\s*=\s*[^;]+;',
        "BILLING_API = '" + CLOUD_URL + "';",
        html, count=1
    )

    # Always replace const base
    html = re.sub(
        r'const base\s*=\s*[^;]+;',
        "const base = '" + CLOUD_URL + "';",
        html, count=1
    )

    # Always remove old login overlay and re-inject fresh one
    # This ensures correct CLOUD_URL is always used
    html = re.sub(r'<div id=["\']rio-login-overlay["\'][\s\S]*?</div>\s*</div>\s*</div>', '', html, count=1)
    html = re.sub(r'<button id=["\']rio-logout-btn["\'][^>]*>[^<]*</button>', '', html, count=1)
    html = re.sub(r'<script>[\s\S]*?var _rioUser[\s\S]*?</script>', '', html, count=1)

    # Replace DOMContentLoaded loadAll block
    dom_ready = (
        "  var _ov=document.getElementById('rio-login-overlay');if(_ov)_ov.style.display='flex';\n"
        "  var _realLoadAll=window.loadAll;\n"
        "  window.loadAll=function(){if(!window._rioUser){var o=document.getElementById('rio-login-overlay');if(o)o.style.display='flex';return;}if(_realLoadAll)_realLoadAll.apply(this,arguments);};\n"
        "  detectPort().then(function(){rioAuthInit();}).catch(function(){rioAuthInit();});"
    )
    if "  loadAll();\n  loadJobs();\n  setInterval(loadAll, 60000);" in html:
        html = html.replace(
            "  loadAll();\n  loadJobs();\n  setInterval(loadAll, 60000);",
            dom_ready
        )

    # Always inject fresh CSS + Login HTML + Auth JS before </body>
    inject = INJECT_CSS + "\n" + LOGIN_HTML + "\n" + AUTH_JS
    last_body = html.rfind("</body>")
    if last_body != -1:
        html = html[:last_body] + inject + "\n</body>" + html[last_body+7:]

    logger.info(f"HTML patched: rio-login-overlay present={'rio-login-overlay' in html}, size={len(html)}")
    return html

# ─────────────────────────────────────────────
#  SERVE HTML DASHBOARD
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    if not os.path.exists(HTML_FILE):
        return HTMLResponse("<h2>Dashboard HTML not found. Place the HTML file next to rio_api.py</h2>", 404)
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()
    html = _apply_patches(html)
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

# ─────────────────────────────────────────────
#  SALES RECORDS
# ─────────────────────────────────────────────
@app.get("/api/sales")
async def get_sales():
    rows = list(col("sales_records").find({}, {"_id": 0}).sort("SNo", DESCENDING))
    return JSONResponse(content=rows)

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
    col("jobs").insert_one({
        "Id":           new_id,
        "Customer":     b.get("Customer", ""),
        "JobName":      b.get("JobName", ""),
        "ConfirmedDate":b.get("ConfirmedDate"),
        "ProductSize":  b.get("ProductSize", ""),
        "Qty":          to_int(b.get("Qty")),
        "Status":       b.get("Status", ""),
        "DispatchDate": b.get("DispatchDate"),
    })
    return ok()

@app.put("/api/jobs/{job_id}")
async def put_jobs(job_id: int, request: Request):
    b = await request.json()
    col("jobs").update_one({"Id": job_id}, {"$set": {
        "Customer":     b.get("Customer", ""),
        "JobName":      b.get("JobName", ""),
        "ConfirmedDate":b.get("ConfirmedDate"),
        "ProductSize":  b.get("ProductSize", ""),
        "Qty":          to_int(b.get("Qty")),
        "Status":       b.get("Status", ""),
        "DispatchDate": b.get("DispatchDate"),
    }})
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
#  AUTH — Login / Users
# ─────────────────────────────────────────────
import secrets

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())

def ensure_default_users():
    """Create default admin user if no users exist."""
    if col("rio_users").count_documents({}) == 0:
        col("rio_users").insert_many([
            {"username": "admin",    "password": hash_password("rio@admin"),  "role": "admin",   "name": "Administrator"},
            {"username": "expense1", "password": hash_password("expense@1"),  "role": "expense", "name": "Expense User 1"},
            {"username": "invoice1", "password": hash_password("invoice@1"),  "role": "invoice", "name": "Invoice User 1"},
        ])
        print("✓ Default users created: admin / expense1 / invoice1")

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
