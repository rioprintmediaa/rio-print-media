v343: login box a little taller.

v343: same as v327 (update-tool fix applies to the PC version only).

v343: shorter login box, everything centred.

v343: Jobs tab - tick many jobs and Mark Completed in one go; Undo Completed (roll back) for ticked jobs or per row. Job card pop-up has a WhatsApp button (job details text; PDF must be attached by hand).

v325: new Rio job card (auto-created with the job, edit pop-up with dropdowns and tick boxes, design image on page 2 of the A4 PDF, saved on the server). Rainbow keeps the old job card.

v325: printed-invoice lock (admin only) with edit log, login lockout (5 tries/10 min), idle logout 60 min.

RIO ERP - files for the ONLINE (web) version, v325

This zip holds the SAME server and page as the PC version (v325); only the PC scripts are not part of it.
How to see which version your online site runs now:  https://YOUR-SITE/api/version  ->  "api_build".

1. BEFORE: log in as admin > Company Details > Database Backup > Backup Database, and keep the file.
2. Upload these files to your GitHub repository (same folder as the current ones) and commit:
      rio_erp_api_v325.py    RIO_PRINT_MEDIA_ERP_v325.html    RIO_MOBILE_v325.html    RIO_STOCK_MOBILE_v325.html
      CMY_Color_Correction_v325.html    CMY_Layer_Proof_PSD_v325.html    Renu_Contacts_v325.html    Debt_and_Loan_v325.html    requirements.txt
3. In Render > your service > Settings, if the Start Command or these variables name the files WITH a version, change it to 281:
      Start Command: uvicorn rio_erp_api_v325:app --host 0.0.0.0 --port $PORT
      HTML_FILE, MOBILE_FILE, STOCK_MOBILE_FILE, CMY_TOOL_FILE, CMY_LAYER_TOOL_FILE, RENU_CONTACTS_FILE, DEBT_LOAN_FILE
   (keep your MONGO_URI and MONGO_DB settings). Render redeploys by itself.
4. Check  https://YOUR-SITE/api/version  shows "api_build":"v325", then press Ctrl+F5 on the site.
   Upload the server and the page TOGETHER: the page needs the new server and shows a "Server update needed" bar otherwise.

WHAT CHANGED IN v325  (the program, i.e. what the web version gains)
  - NEW: the login page now shows "Copyright (c) <year> Rio Print Media. All rights reserved." at the bottom left.
  - NEW: the STATUS bar on the Dashboard shows the version of the page you are looking at ("Page v325"). If the server is a different version, it also shows "Server vNNN", so you can see at once that an update was not installed.
  - CHANGED: RAINBOW Sales Tracker entry: the Eyelets fields are removed (they do not apply to Rainbow); the Packing & Forwarding amount, GST and total stay. Rio is unchanged.
  - Page only: the server is not changed.

WHAT CHANGED IN v322
  - NEW: RAINBOW Sales Tracker entry now has the same product rows as Rio, with the "+ Add Product" button (as many products as you need, each with its own GST %). Eyelets and Packing & Forwarding stay Rio-only and are not shown for Rainbow.
      * orders saved earlier (Product 1 / Product 2) open in the new rows when you edit them; nothing is lost.
      * a new Rainbow order is saved as a list of products; the invoice made from it lists every product with its own GST rate.
  - Page only: the server is not changed.

WHAT CHANGED IN v321
  - FIXED: Round Off on the invoice. The "Round Off" line under CGST / SGST / IGST now shows the real value (for example +0.50 when the total 1942.50 is rounded to 1943.00); it showed 0.00 before. An invoice made from the Sales Tracker is now also rounded to the whole rupee when it is created.
      * the old invoices already saved with paise (for example 1942.50) show the Round Off and the rounded total when printed; the amount saved in the books is not changed.
  - NEW: the GST rate is written next to the tax lines on the invoice / quotation / proforma: "SGST @ 2.5%", "CGST @ 2.5%", "IGST @ 5%" (a line with no tax shows "@ 0%"). Rio and Rainbow.
  - CHANGED: Reports > "Bill Money Not in Indian Bank": for RIO it is still "Bill Money Not in Indian Bank"; for RAINBOW it is now "Money Not in KVB CA" (title, filter text, summary, totals, export file names), and a payment received in KVB CA is no longer listed as "account not specified" for Rainbow.
  - Page only: the server is not changed.

WHAT CHANGED IN v320  (earlier)
  - FIXED (PC version): a RIO Tool window that opened with the message "RIO Tool - Open this tool with RIO_Tool.bat." and then sent you to the normal browser.
      * WHEN: you open a RIO Tool while another one is still open - for example the tool that the update opens at its end, and then you click the RIO Tool icon.
      * CAUSE (reasoned from the symptom; it cannot be reproduced on a computer without Windows): on WINDOWS the option that Python's standard web server sets (SO_REUSEADDR) lets a SECOND program listen
        on a port that is already in use. The second tool got the SAME port as the first, the window of the second tool asked the FIRST tool, which has another secret token and refused. The second tool saw
        no window and after 20 seconds opened the normal browser.
      * NOW: on Windows every tool takes its port EXCLUSIVELY (SO_EXCLUSIVEADDRUSE). A second tool is refused that port, uses the next free one (8766, 8767 ...) and its window talks to it. Both tools work.
      * a window that really belongs to an older tool now says so in plain words (the tool it belonged to was closed or replaced; close the window and open the tool again).
      * Render\logs\rio_tool.log now says why a page was refused (a token that is not this tool's / no token / a wrong Host). The token itself is never written to the log.
  - The server and the page are unchanged (the web version only gets the new version label). The same fix is in the Green Digital (v1.2) and Winner Bags (v43) PC versions.
