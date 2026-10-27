RIO ERP - files for the ONLINE (web) version, v280

This zip holds the SAME server and page as the PC version (v280); only the PC scripts are not part of it.
How to see which version your online site runs now:  https://YOUR-SITE/api/version  ->  "api_build".

1. BEFORE: log in as admin > Company Details > Database Backup > Backup Database, and keep the file.
2. Upload these files to your GitHub repository (same folder as the current ones) and commit:
      rio_erp_api_v280.py    RIO_PRINT_MEDIA_ERP_v280.html    RIO_MOBILE_v280.html    RIO_STOCK_MOBILE_v280.html
      CMY_Color_Correction_v280.html    CMY_Layer_Proof_PSD_v280.html    Renu_Contacts_v280.html    Debt_and_Loan_v280.html    requirements.txt
3. In Render > your service > Settings, if the Start Command or these variables name the files WITH a version, change it to 280:
      Start Command: uvicorn rio_erp_api_v280:app --host 0.0.0.0 --port $PORT
      HTML_FILE, MOBILE_FILE, STOCK_MOBILE_FILE, CMY_TOOL_FILE, CMY_LAYER_TOOL_FILE, RENU_CONTACTS_FILE, DEBT_LOAN_FILE
   (keep your MONGO_URI and MONGO_DB settings). Render redeploys by itself.
4. Check  https://YOUR-SITE/api/version  shows "api_build":"v280", then press Ctrl+F5 on the site.
   Upload the server and the page TOGETHER: the page needs the new server and shows a "Server update needed" bar otherwise.

WHAT CHANGED IN v280  (the program, i.e. what the web version gains)
  - CHANGED: Expense page > Add Expense Entry row:
      * the green + button next to the Sub Category box is removed (Manage Categories does that job).
      * Account box +3px, Amount box +3px.
      * Description takes what is left: it gives up the 6px for Account and Amount and receives the 38px (30px + 8px gap) of the removed + button, so it is 32px wider than before.
      * Date, Category and Sub Category keep EXACTLY the width they had, at every screen width, and no other box on the page was touched.
      * On a screen 1280px wide or narrower the page uses its own small-screen layout for all forms (equal-width boxes); that layout is unchanged.
  - The server is unchanged (API level 3). This is a page change, so the web (Render) version needs the new RIO_PRINT_MEDIA_ERP page file.

EARLIER VERSIONS - changes to the program
v279
  - CHANGED: Dashboard > Overdue Payments now shows the 5 LATEST unpaid sales entries, newest first. Before, it showed the 5 OLDEST unpaid entries (oldest on top), so
    old outstanding amounts stayed on the card and the recent ones never appeared.
      * An entry counts as unpaid while its total is more than the payments received; the amount shown is what is still unpaid.
      * Two entries with the same order date: the one entered last comes first. An entry without a valid date goes last.
      * The financial year chosen at the top still applies (All Years: the latest across all years).
      * "View All Pending" and the Pending page are unchanged (they still list every unpaid entry, grouped by customer).
  - The server is unchanged (API level 3). This is a page change, so the web (Render) version needs the new RIO_PRINT_MEDIA_ERP page file.

v278
  - No change for the web (Render) version: the server and the pages are the same as in v277. The changes are in the PC scripts (see the PC update).

v277
  - No change for the web (Render) version: the server and the pages are the same as in v276. The changes of this version are in the PC scripts (see the PC update):
    the online backup looks every 15 minutes and only acts when something changed, and the server is started automatically after a restart.

v276
  - FIX: an invoice made from a sales entry said "Invoice generated" but could not be found in the Invoice Manager.
    Cause: the invoice NUMBER was taken from the financial year selected at the top (for example 2025-26), but the invoice was DATED today (for example in 2026-27),
    and the Invoice Manager lists an invoice only in the financial year of its date - so it was saved, but sat in the other year's list. (The date was also taken from
    the UTC day, so in India between midnight and 5:30 am it was one day behind.)
    Now the invoice is dated INSIDE the selected year: today when today is in that year (the local date, as before); otherwise the sales entry's own date; otherwise the
    nearest day of that year. After generating, the Invoice Manager is refreshed and the popup shows the date and financial year of the new invoice.
    An invoice that was already made the old way (it sits in another year): press the invoice button of its sales entry - the message names the year, and
    "View in Inv Mgr" switches to that year and searches the invoice.
  - NEW: one sales entry = one invoice. If an invoice was already generated for a sales entry, pressing Generate (either button) shows "Invoice Already Generated"
    with the invoice number, its date and year, and a button for the Invoice Manager. Nothing is created. A new invoice can be generated only after that invoice has been
    deleted (or cancelled): the sales entry is then released. This holds even when the page on screen is old (another tab, another user), on a double click, and when two
    people press the button at the same moment - the server decides (it links the sales entry to the invoice by its Id, and refuses a second invoice with a 409).
  - FIX (found while testing): deleting or cancelling an invoice released the sales entries by the invoice NUMBER only. Numbers restart every financial year, so deleting
    RN05 of 2026-27 could also release last year's sales entry that carries RN05 - which could then be invoiced a second time. Now a sales entry is released by the exact
    invoice Id; older links (number only) are released only within the same financial year. The "already exists" check also uses the exact invoice (not any invoice that
    happens to have the same number in another year).
  - FIX: the "View in Inv Mgr" button of the new popup opens the panel first and then switches the year (the click is only valid before anything is awaited).
  - Server: POST /api/billing/invoices accepts SalesSNo (answers 409 {alreadyGenerated, invoiceNo, invoiceId, invoiceDate, fy}); new GET /api/sales/{sno}/invoice-status;
    delete, cancel and sync-from-sales use the exact link. API level unchanged (3): the new page works with an older server (it falls back to the number) and the old
    page works with the new server - but the full protection needs the new page AND the new server. For the web (Render) version deploy both files.
  - The user guide describes the invoice button of the sales rows and the financial-year rule of the Invoice Manager.

v275
  - FIX: Attendance > RECALCULATE showed "Error: HTTP 404". The page called a URL that does not exist (/api/attendance/att_recalculate); it now calls the right one
    (/api/attendance/recalculate). Every other Attendance call of the page was checked against the server's routes: they are all correct.
  - NEW: Attendance > TEMPLATE is now an EXCEL file (attendance_template.xlsx) with dropdowns, built the same way as the Daily Expense template:
        Name                 dropdown of the employees (the staff plus the names in the records); a new name can still be typed (Excel shows a warning)
        Date                 dropdown of last month's and this month's dates, DD-MM-YYYY; another date can still be typed (a warning)
        Type                 dropdown: Working Day / Sunday Off / Holiday (anything else is refused)
        Job Type             dropdown: Full Time / Part Time (anything else is refused)
        In Time, Out Time    typed by hand (hh:mm, 24-hour) - NO dropdown
    500 rows are ready. The sheet is empty (no sample rows that could be imported by mistake). A hidden Lists sheet holds the lists; a "How to fill" sheet explains them.
    It uses the Excel library that the page already loads for the Daily Expense template (internet is needed once to load it). Without it the button gives the old
    CSV template and says that it has no dropdowns.
  - Attendance > IMPORT CSV / EXCEL now also reads the filled Excel file (.xlsx, .xls), using the reader of the Daily Expense import. It understands the dropdown words and
    DD-MM-YYYY dates, and still the old CSV words (WORK, SUNDAY, HOLIDAY, FULLTIME ...). Duplicates are handled as before (skip or overwrite).
  - The user guide's Attendance chapter now describes TEMPLATE, IMPORT CSV / EXCEL, SHIFTS and DEL EMPLOYEE (which also deletes that employee's attendance records).
  - The server is unchanged (API level 3).

v274
  - FIX (server): after "Restore Database" the next new entry could get a number that was already taken (Id 1, 2, 3 ... again). The number counters are looked up by the
    record's _id, but backup files carry no _id, so a restore lost them; a missing counter then started at 1. The server rebuilt only 12 of its 28 counters at a restart.
    Now: (1) a counter that does not exist starts AFTER the highest number that exists; (2) Restore Database rebuilds all counters from the data afterwards;
    (3) at every server start a counter that is BEHIND its data is raised (never lowered).
    Entries that already got a duplicate number before this fix are not changed by it.
  - The pages are the same as v273 (API level 3). For the web (Render): deploy the new server file (Start Command: uvicorn rio_erp_api_v274:app ...).

v273
  - No change to the program: the server and the pages are the same as v272 (API level 3). Only the PC scripts changed (see the PC zip).

v272
  - No change to the program: the server and the pages are the same as v271 (API level 3). Only the PC scripts changed (see the PC zip).

v271
  - No change to the program: the server and the pages are the same as v270 (API level 3). Only the RIO Tool (PC) changed (see the PC zip).

v270
  - No change to the program: the server and the pages are the same as v269 (API level 3). Only the RIO Tool (PC) changed (see the PC zip).

v269
  - No change to the program: the server and the pages are the same as v268 (API level 3). Only the RIO Tool (PC) changed (see the PC zip).

v268
  - No change to the program: the server and the pages are the same as v267 (API level 3). Only the PC backup scripts changed (see the PC zip).

v267
  - FIX: the Copy buttons for QR pictures (the factory address Maps QR, and the UPI QR) no longer SAVE a picture file when the browser does not allow clipboard
    access. A browser lets a page copy a picture to the clipboard only on a "secure" address (https, http://localhost, http://127.0.0.1, http://*.localhost). On any
    other address (for example the PC's network address) the old code fell back to downloading the picture. Now the picture is shown large with
    "Right-click > Copy image" and a Download button (nothing is saved by itself) and it is also copied as HTML (Word and e-mail paste it).
    On https - your online site - nothing changes: the picture goes straight to the clipboard, as before.
  - The text copies (company details, contacts; main, mobile and contacts pages) now also work on addresses that are not secure.
  - Pages changed: the main page, the mobile page and the contacts page. The server is unchanged (API level 3).

v266
  - No change to the program: the server and the page are the same as v265 (API level 3). Only the RIO Tool's font changed (see the PC zip).

v265
  - No change to the program: the server and the page are the same as v264 (API level 3). The RIO Tool is a PC-only feature (see the PC zip).

v265
  - No change to the program: the server and the page are the same as v264 (API level 3). The RIO Tool is a PC-only feature (see the PC zip).

v264
  - No change to the program: the server and the page are the same as v263 (API level 3). Only the PC online-backup script changed (see the PC zip).

v263
  - No change to the program: the server and the page are the same as v262 (API level 3). Only the PC online-backup scripts changed (see the PC zip).

v262
  - No change to the program: the server and the page are the same as v261 (API level 3). Only the PC online-backup script changed (see the PC zip).

v261
  - No change to the program: the server and the page are the same as v260 (API level 3). The online backup to Google Drive is a PC-only feature (see the PC zip).

v260
  - No change to the program: the server and the page are the same as v259 (API level 3). Only the PC update tool and start/stop scripts changed.

v259
  - No change to the program: the server and the page are the same as v258 (API level 3). The new friendly address is a PC-only feature (see the PC zip).

v259
  - No change to the program: the server and the page are the same as v258 (API level 3). The new friendly address is a PC-only feature (see the PC zip).

v258
  - No change to the program: the server and the page are the same as v257 (API level 3). Only the PC update tool changed (see the PC zip).

v257
  - Sales entry (product rows): the HSN, GST %, Quantity and Rate boxes are wider by 2, 1, 2 and 2 "points" (1 point = 8 px = one
    character): HSN 70 -> 86, GST % 52 -> 60, Qty 64 -> 80, Rate 92 -> 108 px. An 8-digit HSN such as 49111010 now shows in full.
    The same 56 px were taken from the MINIMUM width of the Product and Size columns, so the minimum width of the whole row is unchanged
    and a narrower screen does not start to scroll sideways because of this. On wide screens Product and Size look as before.
  - No server change: the server is the same as v256 (API level 3).

v256
  - FIX: Sales entry - choosing a product left the HSN box empty (the GST % was filled, the HSN was not). Cause: when the product dropdown
    was rewritten in v249 (to cure the empty-dropdown problem) the HSN was left out of the product list. The HSN is filled again when you
    pick a product (also for products whose record uses lower-case field names), and an HSN you type yourself is never overwritten.
  - A sale that was saved or opened while the HSN was missing: when you open it to edit, the blank HSN is filled from the product catalog
    (only blanks are filled; anything typed stays).
  - No server change: the server is the same as v255 (API level 3).

v255
  - Duplicate invoices: when the SAME number exists twice for the SAME customer, an admin gets a "Remove" button (in "Check numbering" and on
    the duplicate rows of the Invoice Manager) instead of "Cancel". The extra copy is removed for good; the other copy keeps the number.
    Duplicates for DIFFERENT customers keep "New number" (or Cancel on the row).
  - Every removed or deleted bill is saved WITH its lines in the new table "removed_invoices" (it is part of the backup too), so it can be recovered.
  - "Delete" on the LAST bill of a series works again: the button is no longer faded for a printed bill. A printed bill needs an admin and a
    stronger confirmation; a last bill that was never printed can be deleted by anyone, as before. Older bills still cannot be deleted - cancel
    them. After a delete the number is free again for the next invoice.
  - The server now reports API level 3: upload rio_erp_api_v255.py and the page together.

v251 - v254
  - No change to the program (PC scripts and packaging only).

v250 (also delivered as v250.1)
  - Invoice form: "Update Invoice" and "Save & Print" work again (page fix, and the server route that saves an edit had lost its route line).
  - Duplicate invoice numbers are shown in the Invoice Manager (orange DUPLICATE mark); "Check numbering" lists them with New number / Cancel;
    a cancelled copy is no longer a duplicate.
  - Edit opens the exact invoice by its id; a quick double click on Save creates only one invoice.
  - The server reports API level 2: page and server must be uploaded together.

v249 (also v249.1 - v249.3)
  - Sales entry: the Product dropdown refreshes when you open the screen and says why it is empty.
  - Server: "Invoice not found" on update fixed; the quotation mobile-visibility toggle fixed for quotations without the flag.
  - Server: an encrypted (TLS) connection only for Atlas / non-local MongoDB - no change for the online site.

v248 and earlier
  - Cancel instead of delete for printed invoices, the numbering check, duplicate-customer merge, admin protection (v244) and more:
    see Rio_ERP_Changelog_vNNN.xlsx in the zip.
