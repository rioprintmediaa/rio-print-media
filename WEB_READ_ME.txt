RIO ERP - files for the ONLINE (web) version, v293

This zip holds the SAME server and page as the PC version (v293); only the PC scripts are not part of it.
How to see which version your online site runs now:  https://YOUR-SITE/api/version  ->  "api_build".

1. BEFORE: log in as admin > Company Details > Database Backup > Backup Database, and keep the file.
2. Upload these files to your GitHub repository (same folder as the current ones) and commit:
      rio_erp_api_v293.py    RIO_PRINT_MEDIA_ERP_v293.html    RIO_MOBILE_v293.html    RIO_STOCK_MOBILE_v293.html
      CMY_Color_Correction_v293.html    CMY_Layer_Proof_PSD_v293.html    Renu_Contacts_v293.html    Debt_and_Loan_v293.html    requirements.txt
3. In Render > your service > Settings, if the Start Command or these variables name the files WITH a version, change it to 281:
      Start Command: uvicorn rio_erp_api_v293:app --host 0.0.0.0 --port $PORT
      HTML_FILE, MOBILE_FILE, STOCK_MOBILE_FILE, CMY_TOOL_FILE, CMY_LAYER_TOOL_FILE, RENU_CONTACTS_FILE, DEBT_LOAN_FILE
   (keep your MONGO_URI and MONGO_DB settings). Render redeploys by itself.
4. Check  https://YOUR-SITE/api/version  shows "api_build":"v293", then press Ctrl+F5 on the site.
   Upload the server and the page TOGETHER: the page needs the new server and shows a "Server update needed" bar otherwise.

WHAT CHANGED IN v293  (the program, i.e. what the web version gains)
  - NEW: a page that was opened BEFORE an update now tells you. A blue bar at the top says "A new version (v293) has been installed. This page is still v292." with a Reload button.
    Until now an open page kept running the OLD code: a new card (for example Bonus Details) or a new column did not appear until the page was reloaded (Ctrl+F5), and nothing said so.
      * the page asks the server which version it is 15 seconds after it opens, every 2 minutes, and when you come back to the window. It is shown to everybody (not only the admin).
      * the cross hides it for this session; it comes back only for a version that is newer still. When the page is up to date it disappears by itself.
  - FIXED: the ADMIN login always sees every card of Company/Bank Details. Before, a saved list of allowed sections (made in the role settings) hid every card that was not on the list - the
    admin's card too - and a card that is new is never on an old list, so Bonus Details could stay hidden. Other roles are unchanged: tick Bonus Details for them in User Management (roles).
  - The server now sends the pages with "Cache-Control: no-cache", so the browser asks the server again instead of showing an old copy.
  - The server and the page changed: for the web (Render) version deploy BOTH files.
