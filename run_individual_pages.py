#!/usr/bin/env python3
"""
Script để chạy từng page Streamlit trên các port khác nhau
"""

import subprocess
import sys
import time
import signal
import os

def kill_port(port):
    """Kill process running on specific port"""
    try:
        subprocess.run(f"lsof -ti:{port} | xargs kill -9", shell=True, capture_output=True)
    except:
        pass

def run_page(page_path, port, name):
    """Run a single Streamlit page"""
    print(f"🚀 Starting {name} on http://localhost:{port}")
    cmd = f"streamlit run {page_path} --server.port {port} --server.headless true"
    return subprocess.Popen(cmd, shell=True)

def main():
    pages = [
        ("pages/1_Company_Dashboard.py", 8503, "Company Dashboard 📈"),
        ("pages/2_Market_Overview.py", 8504, "Market Overview 📊"),
        ("pages/2_Technical_Analysis.py", 8505, "Technical Analysis 📉"),
        ("pages/3_🔍_Stock_Screener.py", 8506, "Stock Screener 🔍"),
        ("pages/4_⚙️_Settings.py", 8507, "Settings ⚙️")
    ]
    
    # Clean up existing processes
    print("🧹 Cleaning up existing processes...")
    for _, port, _ in pages:
        kill_port(port)
    
    time.sleep(2)
    
    # Start all pages
    processes = []
    print("\n" + "="*50)
    for page_path, port, name in pages:
        if os.path.exists(page_path):
            proc = run_page(page_path, port, name)
            processes.append(proc)
        else:
            print(f"⚠️ Warning: {page_path} not found")
    
    print("\n" + "="*50)
    print("\n✅ All pages are starting up...")
    print("\nAccess your pages at:")
    print("  📈 Company Dashboard:  http://localhost:8503")
    print("  📊 Market Overview:    http://localhost:8504")
    print("  📉 Technical Analysis: http://localhost:8505")
    print("  🔍 Stock Screener:     http://localhost:8506")
    print("  ⚙️  Settings:          http://localhost:8507")
    print("\nPress Ctrl+C to stop all servers")
    print("="*50 + "\n")
    
    # Handle Ctrl+C
    def signal_handler(sig, frame):
        print("\n\n🛑 Stopping all servers...")
        for proc in processes:
            proc.terminate()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Wait for all processes
    try:
        for proc in processes:
            proc.wait()
    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    main()