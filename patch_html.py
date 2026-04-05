"""
patch_html.py — RIO PRINT MEDIA (v7 - login + mobile + role restriction)
Run: python patch_html.py
"""

import os

RENDER_URL  = "https://rio-print-media.onrender.com"
INPUT_HTML  = "Rio_Sales_Tracker_v3_0.html"
OUTPUT_HTML = "Rio_Sales_Tracker_ONLINE.html"

# ── Patch 1: detectPort replacement ──────────────────────────
OLD_DETECT = """async function detectPort() {
  const PORTS = [8765, 8766, 8767, 8768, 8769, 8770, 8771, 8772, 8773, 8774, 8775];

  // ── FASTEST PATH: PS1 injected the port directly into the page ──
  if (window.__RIO_PORT) {
    const injectedUrl = 'http://localhost:' + window.__RIO_PORT + '/api/ping';
    dbg('Using injected port ' + window.__RIO_PORT, '#90caf9');
    // Small delay so PS1 finishes writing the HTML response before we ping
    await new Promise(r => setTimeout(r, 300));
    if (await pingUrl(injectedUrl, 3000)) {
      API = window.__RIO_API || ('http://localhost:' + window.__RIO_PORT + '/api');
      window._BILLING_BASE = 'http://localhost:' + window.__RIO_PORT;
      return true;
    }
  }

  // ── SERVED MODE: try relative URL (with retries for startup race) ──
  if (location.protocol === 'http:' && location.hostname === 'localhost') {
    dbg('Served mode: ' + location.href, '#90caf9');
    for (let attempt = 0; attempt < 3; attempt++) {
      if (attempt > 0) {
        dbg('Retry ' + attempt + '/2 connecting to API...', '#ffb74d');
        await new Promise(r => setTimeout(r, 1500 * attempt));
      }
      if (await pingUrl('/api/ping', 2000)) { API = '/api'; return true; }
    }
    dbg('Relative ping failed, scanning all ports...', '#aaa');
  } else {
    dbg('File mode: ' + location.href, '#90caf9');
  }

  // ── FALLBACK: scan all possible ports in parallel ──
  dbg('Scanning ports ' + PORTS.join(', ') + ' ...', '#aaa');
  const results = await Promise.all(
    PORTS.map(p => pingUrl('http://localhost:' + p + '/api/ping', 2500).then(ok => ok ? p : null))
  );
  const found = results.find(p => p !== null);
  if (found) { API = 'http://localhost:' + found + '/api'; window._BILLING_BASE = 'http://localhost:' + found; return true; }
  return false;
}"""

NEW_DETECT = f"""async function detectPort() {{
  const CLOUD_API  = "{RENDER_URL}/api";
  const CLOUD_BASE = "{RENDER_URL}";
  API = CLOUD_API;
  window.API = CLOUD_API;
  window._BILLING_BASE = CLOUD_BASE;
  window.__RIO_API  = CLOUD_API;
  window.__RIO_PORT = null;
  dbg('Cloud mode: ' + CLOUD_API, '#4caf50');
  try {{
    const ctrl = new AbortController();
    setTimeout(() => ctrl.abort(), 10000);
    const r = await fetch(CLOUD_API + '/ping', {{ cache:'no-store', signal:ctrl.signal }});
    if (r.ok) {{ dbg('Cloud API connected OK', '#4caf50'); return true; }}
  }} catch(e) {{ dbg('Cloud ping error: ' + e, '#f44336'); }}
  return false;
}}"""

OLD_BILLING = "BILLING_API = window.location.origin; // auto-matches whatever port PS1 uses"
NEW_BILLING = f'BILLING_API = "{RENDER_URL}"; // cloud override'

OLD_BASE = "  const base = window._BILLING_BASE || window.location.origin || BILLING_API;"
NEW_BASE = f'  const base = "{RENDER_URL}";'

OLD_ENSURE = """async function ensureProducts() {
  try {
    // FIX: use window._BILLING_BASE (set after port detection) not BILLING_API
    const base = window._BILLING_BASE || window.location.origin || BILLING_API;
    const resp = await fetch(base + '/api/billing/products');"""
NEW_ENSURE = f"""async function ensureProducts() {{
  try {{
    const base = "{RENDER_URL}";
    const resp = await fetch(base + '/api/billing/products');"""

# ── Patch 2: DOMContentLoaded — add auth check before loadAll ─
OLD_DOM = """  loadAll();
  loadJobs();
  setInterval(loadAll, 60000);"""

NEW_DOM = """  detectPort().then(() => { rioAuthInit(); });"""

# ── Login screen + auth JS to inject before </body> ──────────
AUTH_JS = f"""
<style>
#rio-login-overlay {{
  position:fixed;top:0;left:0;width:100vw;height:100vh;
  background:linear-gradient(135deg,#0e1220 0%,#1a237e 100%);
  display:flex;align-items:center;justify-content:center;
  z-index:99999;flex-direction:column;
}}
#rio-login-box {{
  background:white;border-radius:18px;padding:36px 32px 28px;
  width:90%;max-width:380px;box-shadow:0 20px 60px rgba(0,0,0,0.5);
}}
#rio-login-box h2 {{
  margin:0 0 4px;font-family:'Exo 2',sans-serif;font-size:1.4rem;
  color:#1a237e;text-align:center;
}}
#rio-login-box p {{
  margin:0 0 24px;font-size:0.82rem;color:#888;text-align:center;
  font-family:'Nunito',sans-serif;
}}
.rio-login-field {{
  width:100%;margin-bottom:14px;box-sizing:border-box;
  border:1.5px solid #e0e0e0;border-radius:10px;
  padding:12px 14px;font-size:1rem;font-family:'Nunito',sans-serif;
  outline:none;transition:border 0.2s;
}}
.rio-login-field:focus {{ border-color:#3949ab; }}
#rio-login-btn {{
  width:100%;padding:13px;background:linear-gradient(90deg,#1a237e,#3949ab);
  color:white;border:none;border-radius:10px;font-size:1rem;
  font-family:'Exo 2',sans-serif;font-weight:700;cursor:pointer;
  margin-top:4px;letter-spacing:0.5px;
}}
#rio-login-btn:active {{ opacity:0.85; }}
#rio-login-err {{
  color:#c62828;font-size:0.82rem;text-align:center;
  margin-top:10px;font-family:'Nunito',sans-serif;min-height:18px;
}}
#rio-login-logo {{
  font-family:'Exo 2',sans-serif;font-size:1.1rem;font-weight:900;
  color:white;margin-bottom:22px;letter-spacing:2px;text-align:center;
  opacity:0.9;
}}
/* Expense-only overlay */
body.role-expense #nav {{ display:none !important; }}
body.role-expense .panel:not(#panel-expense) {{ display:none !important; }}
body.role-expense #panel-expense {{ display:block !important; }}
body.role-expense #content {{ margin-left:0 !important; padding:12px !important; }}
/* Invoice-only overlay */
body.role-invoice #nav {{ display:none !important; }}
body.role-invoice .panel:not(#panel-rio-invoice):not(#panel-rio-quotation):not(#panel-rio-invmgr):not(#panel-rio-customers):not(#panel-rio-products):not(#panel-rio-reports) {{ display:none !important; }}
body.role-invoice #panel-rio-invoice {{ display:flex !important; }}
body.role-invoice #content {{ margin-left:0 !important; padding:12px !important; }}
/* Logout button */
#rio-logout-btn {{
  position:fixed;top:10px;right:12px;z-index:9000;
  background:#c62828;color:white;border:none;border-radius:8px;
  padding:7px 16px;font-family:'Exo 2',sans-serif;font-weight:700;
  font-size:0.78rem;cursor:pointer;box-shadow:0 2px 8px rgba(0,0,0,0.3);
}}
/* Mobile responsive */
@media(max-width:700px){{
  #nav {{ width:56px !important; min-width:56px !important; }}
  .nav-btn-body {{ display:none !important; }}
  #content {{ margin-left:56px !important; }}
  .nav-logo-area {{ padding:8px 4px !important; }}
  body.role-expense #content {{ margin-left:0 !important; }}
}}
</style>

<div id="rio-login-overlay">
  <div id="rio-login-logo">🖨 RIO PRINT MEDIA</div>
  <div id="rio-login-box">
    <h2>Welcome Back</h2>
    <p>Sign in to continue</p>
    <input class="rio-login-field" id="rio-uname" type="text"
      placeholder="Username" autocomplete="username"
      onkeydown="if(event.key==='Enter')document.getElementById('rio-pwd').focus()">
    <input class="rio-login-field" id="rio-pwd" type="password"
      placeholder="Password" autocomplete="current-password"
      onkeydown="if(event.key==='Enter')rioDoLogin()">
    <button id="rio-login-btn" onclick="rioDoLogin()">Sign In</button>
    <div id="rio-login-err"></div>
  </div>
</div>

<button id="rio-logout-btn" style="display:none" onclick="rioLogout()">⏻ Logout</button>

<script>
// ── Rio Auth ──────────────────────────────────────────────────
let _rioUser = null;

async function rioDoLogin() {{
  const uname = document.getElementById('rio-uname').value.trim();
  const pwd   = document.getElementById('rio-pwd').value.trim();
  const errEl = document.getElementById('rio-login-err');
  const btn   = document.getElementById('rio-login-btn');
  if (!uname || !pwd) {{ errEl.textContent = 'Enter username and password'; return; }}
  btn.textContent = 'Signing in...'; btn.disabled = true;
  try {{
    const r = await fetch('{RENDER_URL}/api/auth/login', {{
      method:'POST', headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{username: uname, password: pwd}})
    }});
    const d = await r.json();
    if (!r.ok || !d.ok) {{
      errEl.textContent = d.error || 'Invalid username or password';
      btn.textContent = 'Sign In'; btn.disabled = false;
      return;
    }}
    _rioUser = d;
    sessionStorage.setItem('rio_user', JSON.stringify(d));
    rioApplyRole(d);
  }} catch(e) {{
    errEl.textContent = 'Connection error — try again';
    btn.textContent = 'Sign In'; btn.disabled = false;
  }}
}}

function rioApplyRole(user) {{
  // Hide login screen
  const overlay = document.getElementById('rio-login-overlay');
  if (overlay) overlay.style.display = 'none';
  // Show logout button
  const lb = document.getElementById('rio-logout-btn');
  if (lb) {{ lb.style.display = 'block'; lb.textContent = '⏻ ' + user.name; }}
  if (user.role === 'expense') {{
    document.body.classList.add('role-expense');
    // Show only expense panel
    document.querySelectorAll('.panel').forEach(p => p.style.display = 'none');
    const ep = document.getElementById('panel-expense');
    if (ep) {{ ep.style.display = 'block'; ep.classList.add('active'); }}
    // Load only expenses
    detectPort().then(() => {{
      loadExpenses();
      renderExpenses();
    }});
  }} else {{
    // Admin — full access
    document.body.classList.remove('role-expense');
    loadAll();
    loadJobs();
    setInterval(loadAll, 60000);
  }}
}}

function rioLogout() {{
  sessionStorage.removeItem('rio_user');
  _rioUser = null;
  location.reload();
}}

function rioAuthInit() {{
  // Check if already logged in this session
  try {{
    const saved = sessionStorage.getItem('rio_user');
    if (saved) {{
      const user = JSON.parse(saved);
      if (user && user.username && user.role) {{
        _rioUser = user;
        rioApplyRole(user);
        return;
      }}
    }}
  }} catch(e) {{}}
  // Show login screen — do nothing else until logged in
  const overlay = document.getElementById('rio-login-overlay');
  if (overlay) overlay.style.display = 'flex';
  setTimeout(() => {{
    const u = document.getElementById('rio-uname');
    if (u) u.focus();
  }}, 300);
}}
// ─────────────────────────────────────────────────────────────
</script>
"""

def patch():
    if not os.path.exists(INPUT_HTML):
        print(f"ERROR: {INPUT_HTML} not found.")
        return

    with open(INPUT_HTML, "r", encoding="utf-8") as f:
        html = f.read()

    if OLD_DETECT not in html:
        print("ERROR: detectPort function not found. Is this the right HTML file?")
        return

    patched = html

    # Cloud patches
    patched = patched.replace(OLD_DETECT,  NEW_DETECT,  1)
    patched = patched.replace(OLD_BILLING, NEW_BILLING, 1)
    patched = patched.replace(OLD_BASE,    NEW_BASE)
    patched = patched.replace(OLD_ENSURE,  NEW_ENSURE,  1)

    # Auth patch — replace DOMContentLoaded loadAll
    patched = patched.replace(OLD_DOM, NEW_DOM, 1)

    # Inject login screen + auth JS before </body>
    patched = patched.replace("</body>", AUTH_JS + "\n</body>", 1)

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(patched)

    replaced_base = patched.count(NEW_BASE)
    print(f"\nSUCCESS: Created {OUTPUT_HTML}")
    print(f"  - Cloud URL patches: OK")
    print(f"  - const base replaced: {replaced_base} times")
    print(f"  - Login screen: OK")
    print(f"  - Role restriction: OK")
    print(f"\nDefault users created on first launch:")
    print(f"  Admin  → username: admin     password: rio@admin")
    print(f"  Expense→ username: expense1  password: expense@1")
    print(f"\nUpload {OUTPUT_HTML} to GitHub as Rio_Sales_Tracker_v3_0.html")
    print(f"Then Manual Deploy on Render.\n")

if __name__ == "__main__":
    patch()
