# 🚀 Meme Coin Analyzer

A simple tool to analyze meme coins with the long-term goal of training models to optimize transactions.

## 📦 Setup

### 1. Environment Configuration

-   Copy `.env.example` into a new file named `.env`.
-   Fill in the required values (API keys, etc.).

### 2. Create & Activate Virtual Environment

```bash
# Create a virtual environment
python -m venv .venv
```

**On Windows (PowerShell):**

```powershell
.\.venv\Scripts\Activate.ps1
```

**On Linux / macOS (bash/zsh):**

```bash
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## ▶️ Running the App

Start the Streamlit application with:

```bash
streamlit run ./src/0_Home.py
```

## 🛠️ Tech Stack

-   **Python** – Data analysis & model building
-   **Streamlit** – Interactive UI
-   **.env** – Environment variable management

## 📌 Notes

-   Make sure your `.env` file is properly configured before launching the app.
-   This project is a **prototype** for exploring meme coin market patterns and building ML-powered transaction optimization.
