# Rio Sales Tracker - Render Deployment Guide

## 📦 Repository Setup

### 1. GitHub Repository
Create a new GitHub repository and push your code:

```bash
cd /app
git init
git add .
git commit -m "Initial commit - Rio Sales Tracker FY 2026-27"
git remote add origin YOUR_GITHUB_REPO_URL
git push -u origin main
```

## 🚀 Render Deployment

### 2. Create Web Service on Render

1. **Go to**: https://dashboard.render.com/
2. **Click**: "New +" → "Web Service"
3. **Connect**: Your GitHub repository
4. **Configure**:

**Basic Settings:**
- **Name**: `rio-sales-tracker` (or your choice)
- **Region**: Choose closest to your location
- **Branch**: `main`
- **Root Directory**: Leave empty
- **Runtime**: `Python 3`
- **Build Command**: 
  ```
  pip install -r requirements.txt
  ```
- **Start Command**:
  ```
  cd backend && uvicorn server:app --host 0.0.0.0 --port $PORT
  ```

**Environment Variables** (Click "Advanced" → "Add Environment Variable"):

| Key | Value |
|-----|-------|
| `MONGO_URI` | `mongodb+srv://rioprintmediaa_db_Mani:Nannavinam%401212@rioprintmedia.6e7l1ak.mongodb.net/RioPrintMedia?appName=RIOPRINTMEDIA` |
| `MONGO_DB` | `RioPrintMedia` |
| `CORS_ORIGINS` | `*` |
| `PYTHON_VERSION` | `3.11.0` |

**Instance Type:**
- **Free** tier (for testing)
- **Starter** ($7/month - recommended for production)

### 3. Deploy

1. Click **"Create Web Service"**
2. Wait for build to complete (~3-5 minutes)
3. Render will provide your URL: `https://rio-sales-tracker.onrender.com`

### 4. Update Frontend API URL

After deployment, update the frontend to use your Render URL:

**Option A - Via Environment Variable (Recommended):**
Add to Render Environment Variables:
```
RENDER_URL=https://your-app-name.onrender.com
```

**Option B - Hardcode (Quick fix):**
The app already uses `window.location.origin`, so it will automatically work with any domain!

## ✅ Post-Deployment Checklist

### Test All Features:
1. **Dashboard**: Sales, expenses, jobs loading
2. **Sales Tracker**: Add/edit sales
3. **Customer Bill**: Invoice generation
4. **Daily Expense**: Add expenses
5. **Reports**: All 8 report cards working
6. **Account Ledger**: Opening balances, transactions
7. **Jobs**: Add/view/delete jobs
8. **FY Selection**: Switch between FYs

### Database Verification:
```bash
# Check if data persists
curl https://your-app-name.onrender.com/api/ping
curl https://your-app-name.onrender.com/api/sales
curl https://your-app-name.onrender.com/api/customers
```

## 🔧 Troubleshooting

### Issue: App not loading
- Check Render logs: Dashboard → Logs
- Verify MongoDB connection string is correct
- Ensure all environment variables are set

### Issue: Database connection error
- Check MongoDB Atlas IP whitelist (allow 0.0.0.0/0 for Render)
- Verify MONGO_URI is URL-encoded

### Issue: Static files not loading
- Render serves the HTML from FastAPI directly
- No static file configuration needed

## 📁 Files Structure (Already Set Up)

```
/app/
├── backend/
│   ├── server.py              # Main FastAPI app
│   ├── .env                   # Environment variables (for local)
│   └── Rio_Sales_Tracker_ONLINE.html  # Frontend HTML
├── requirements.txt           # Python dependencies
└── cleanup_fy_2026_27.py     # Database cleanup script
```

## 🔐 MongoDB Atlas Setup (Already Done)

Your MongoDB connection is:
- **Cluster**: rioprintmedia.6e7l1ak.mongodb.net
- **Database**: RioPrintMedia
- **Status**: ✅ Connected and working

## 📊 Current Data Status

**FY 2026-27 (Clean Database):**
- Sales: 4 records
- Customers: 466 records
- Expenses: 82 records (filtered to FY)
- Jobs: 38 records
- Ledger: 27 entries (synced)

## 🎯 Next Steps After Deployment

1. **Test on Render URL**: Verify all features work
2. **Set up custom domain** (optional): Add CNAME in Render settings
3. **Enable HTTPS**: Automatic on Render
4. **Backup database**: MongoDB Atlas → Export data
5. **Monitor usage**: Render Dashboard → Metrics

## ⚠️ Important Notes

- **Free tier sleeps after 15 min of inactivity** (first request will be slow)
- **Upgrade to Starter** ($7/mo) for always-on service
- **MongoDB Atlas free tier**: 512MB storage limit
- **CORS is open** (`*`) - restrict in production if needed

## 🆘 Support

If deployment fails:
1. Check Render build logs
2. Verify `requirements.txt` has all dependencies
3. Test locally first: `cd backend && uvicorn server:app --reload`
4. MongoDB connection: Test with `mongosh` command

---

**Deployment ready! Your Rio Sales Tracker will be live in ~5 minutes after clicking "Create Web Service" on Render.**
