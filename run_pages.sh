#!/bin/bash

# Script để chạy các pages trên các port khác nhau

echo "🚀 Starting Stock Dashboard Pages on different ports..."

# Kill existing processes if any
echo "Cleaning up existing processes..."
lsof -ti:8503 | xargs kill -9 2>/dev/null
lsof -ti:8504 | xargs kill -9 2>/dev/null
lsof -ti:8505 | xargs kill -9 2>/dev/null
lsof -ti:8506 | xargs kill -9 2>/dev/null
lsof -ti:8507 | xargs kill -9 2>/dev/null

sleep 2

# Run Company Dashboard on port 8503
echo "📈 Starting Company Dashboard on http://localhost:8503"
streamlit run pages/1_Company_Dashboard.py --server.port 8503 --server.headless true &

# Run Market Overview on port 8504
echo "📊 Starting Market Overview on http://localhost:8504"
streamlit run "pages/2_Market_Overview.py" --server.port 8504 --server.headless true &

# Run Technical Analysis on port 8505
echo "📉 Starting Technical Analysis on http://localhost:8505"
streamlit run pages/2_Technical_Analysis.py --server.port 8505 --server.headless true &

# Run Stock Screener on port 8506
echo "🔍 Starting Stock Screener on http://localhost:8506"
streamlit run "pages/3_🔍_Stock_Screener.py" --server.port 8506 --server.headless true &

# Run Settings on port 8507
echo "⚙️ Starting Settings on http://localhost:8507"
streamlit run "pages/4_⚙️_Settings.py" --server.port 8507 --server.headless true &

echo ""
echo "✅ All pages are starting up..."
echo ""
echo "Access your pages at:"
echo "  📈 Company Dashboard: http://localhost:8503"
echo "  📊 Market Overview:   http://localhost:8504"
echo "  📉 Technical Analysis: http://localhost:8505"
echo "  🔍 Stock Screener:    http://localhost:8506"
echo "  ⚙️ Settings:          http://localhost:8507"
echo ""
echo "Press Ctrl+C to stop all servers"

# Wait for all background processes
wait