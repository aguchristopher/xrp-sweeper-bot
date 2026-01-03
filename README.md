# XRP Sweeper Bot

A real-time XRP sweeper bot that listens for incoming transactions and automatically forwards funds to a secure destination address.

## Features
- **Mnemonic & Seed Support**: Works with 12/24-word phrases or standard family seeds (s-seeds).
- **Real-time Sweeping**: Uses WebSockets to detect incoming transactions instantly.
- **Safety First**: Leaves a minimum 10 XRP reserve to keep the account active.

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   - Copy `.env.example` to `.env`.
   - Add your mnemonic phrase or seed to `XRPL_SEED`.
   - Add your destination wallet address to `DESTINATION`.

3. **Run the Bot**:
   ```bash
   python app.py
   ```

## Disclaimer
This software is for educational purposes only. Always test with small amounts first. Use at your own risk.
