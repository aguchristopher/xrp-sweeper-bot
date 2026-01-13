import os
import asyncio
from dotenv import load_dotenv

load_dotenv()
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.wallet import Wallet
from xrpl.models.requests import Subscribe, AccountInfo
from xrpl.models.transactions import Payment
from xrpl.transaction import sign
from xrpl.asyncio.transaction import autofill, submit_and_wait
from xrpl.utils import drops_to_xrp

# ===== CONFIGURATION =====
XRPL_WS = "wss://xrplcluster.com"
# XRPL_WS = "wss://s.altnet.rippletest.net:51233"  # Uncomment for Testnet

WALLET_SEED = os.getenv("XRPL_SEED")
DESTINATION = os.getenv("DESTINATION")

# Validate Environment Variables
if not WALLET_SEED or not WALLET_SEED.startswith("s"):
    raise ValueError("Error: XRPL_SEED must be set and start with 's'")
if not DESTINATION:
    raise ValueError("Error: DESTINATION address must be set")

# ===== XRPL CONSTANTS (2025 Standards) =====
# 1 XRP = 1,000,000 Drops
BASE_RESERVE_DROPS = 1_000_000  # 1 XRP required to activate account
OWNER_RESERVE_DROPS = 200_000  # 0.2 XRP per object (trustline, offer, etc.)
FEE_BUFFER_DROPS = 50  # buffer to cover network fees (usually 10-12 drops)
MIN_SWEEP_DROPS = 100_000  # Minimum balance (0.1 XRP) required to trigger sweep
# ===========================================


def wallets_from_seed(seed):
    """
    Generates both SECP256K1 and ED25519 wallets from the same seed
    to ensure we find the correct account on the ledger.
    """
    wallet_secp = Wallet.from_seed(seed, algorithm="secp256k1")
    wallet_ed = Wallet.from_seed(seed, algorithm="ed25519")
    return wallet_secp, wallet_ed


async def get_account_data_drops(client, address):
    """
    Fetches account info and calculates exact spendable drops (integers).
    Returns None if account is not funded.
    """
    try:
        req = AccountInfo(account=address,
                          ledger_index="validated",
                          strict=True)
        resp = await client.request(req)

        if not resp.result or "account_data" not in resp.result:
            return None

        data = resp.result["account_data"]
        balance_drops = int(data["Balance"])
        owner_count = data.get("OwnerCount", 0)

        # Calculate Reserve
        reserve_drops = BASE_RESERVE_DROPS + (OWNER_RESERVE_DROPS *
                                              owner_count)

        # Calculate Spendable (Balance - Reserve - Safety Buffer)
        spendable_drops = balance_drops - reserve_drops - FEE_BUFFER_DROPS

        return {
            "balance_drops": balance_drops,
            "reserve_drops": reserve_drops,
            "spendable_drops": spendable_drops
        }
    except Exception:
        # If account doesn't exist yet
        return None


async def perform_sweep(client, wallet, destination, lock):
    """
    Safely sweeps funds. Uses a lock to prevent concurrent sweep attempts.
    """
    if lock.locked():
        print(">> Sweep already in progress. Skipping...")
        return

    async with lock:
        data = await get_account_data_drops(client, wallet.classic_address)

        if not data:
            print(">> Error: Could not fetch account data.")
            return

        spendable = data["spendable_drops"]

        # 1. Safety Check: Negative Spendable
        # If balance < reserve, spendable will be negative. We cannot convert negative to XRP.
        if spendable <= 0:
            print(f">> Funds locked by Reserve (1 XRP). Spendable: 0 XRP")
            return

        # 2. Threshold Check
        if spendable < MIN_SWEEP_DROPS:
            print(
                f">> Balance too low to sweep. Spendable: {drops_to_xrp(str(spendable))} XRP"
            )
            return

        print(
            f">> Sweeping {drops_to_xrp(str(spendable))} XRP to {destination}..."
        )

        # 3. Build Transaction
        payment = Payment(
            account=wallet.classic_address,
            destination=destination,
            amount=str(spendable)  # Amount must be string of drops
        )

        try:
            # autofill handles Sequence and Fee calculation
            prepared = await autofill(payment, client)
            signed = sign(prepared, wallet)
            result = await submit_and_wait(signed, client)

            tx_result = result.result.get("meta",
                                          {}).get("TransactionResult",
                                                  "UNKNOWN")

            if tx_result == "tesSUCCESS":
                print(f">> SUCCESS: Transferred funds. Result: {tx_result}")
            else:
                print(f">> FAILURE: Transaction failed. Result: {tx_result}")

        except Exception as e:
            print(f">> Sweep Error: {e}")


async def main():
    wallet_secp, wallet_ed = wallets_from_seed(WALLET_SEED)
    sweep_lock = asyncio.Lock()

    async with AsyncWebsocketClient(XRPL_WS) as client:
        print(f"Connected to {XRPL_WS}")

        # 1. Determine Active Wallet
        print("Checking account status...")
        data_secp = await get_account_data_drops(client,
                                                 wallet_secp.classic_address)
        data_ed = await get_account_data_drops(client,
                                               wallet_ed.classic_address)

        if data_ed:
            wallet = wallet_ed
            print(f"Active Wallet (ED25519): {wallet.classic_address}")
        elif data_secp:
            wallet = wallet_secp
            print(f"Active Wallet (SECP256K1): {wallet.classic_address}")
        else:
            print(
                f"Account {wallet_secp.classic_address} not found (not funded)."
            )
            print("Waiting for initial funding (needs > 1 XRP)...")
            wallet = wallet_secp

        # 2. Subscribe to Ledger Updates
        print(f"Subscribing to updates for {wallet.classic_address}...")
        await client.request(Subscribe(accounts=[wallet.classic_address]))

        # 3. Initial Sweep Check (Clear out existing funds on startup)
        if data_ed or data_secp:
            await perform_sweep(client, wallet, DESTINATION, sweep_lock)

        # 4. Main Event Loop
        print("Listening for incoming transactions...")
        async for msg in client:
            # Only process validated ledger messages
            if not msg.get("validated", False):
                continue

            # PARSING FIX: Handle both 'transaction' and 'tx_json' formats
            tx = None
            if "transaction" in msg:
                tx = msg["transaction"]
            elif "tx_json" in msg:
                tx = msg["tx_json"]

            if not tx:
                continue

            # FILTER: Payment + Destination is Us + Account is NOT Us
            if (tx.get("TransactionType") == "Payment"
                    and tx.get("Destination") == wallet.classic_address
                    and tx.get("Account") != wallet.classic_address):

                # Extract amount (Check delivered_amount if available for accuracy)
                amount = tx.get("Amount")
                if "meta" in msg and "delivered_amount" in msg["meta"]:
                    amount = msg["meta"]["delivered_amount"]

                # Ignore Token Payments (Non-XRP payments are dicts)
                if isinstance(amount, dict):
                    print(
                        "Token payment received. Ignoring (XRP only script).")
                    continue

                print(f"Incoming Payment Detected: {drops_to_xrp(amount)} XRP")

                # Small delay to allow the node to update AccountInfo state
                await asyncio.sleep(1)

                # Launch sweep in background
                asyncio.create_task(
                    perform_sweep(client, wallet, DESTINATION, sweep_lock))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopping sweeper...")
