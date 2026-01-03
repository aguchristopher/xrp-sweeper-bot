import os
import asyncio
import base64
from dotenv import load_dotenv
from xrpl.asyncio.clients import AsyncWebsocketClient
from mnemonic import Mnemonic

# Load environment variables from .env file
load_dotenv()
from xrpl.wallet import Wallet
from xrpl.models.requests import Subscribe, AccountInfo
from xrpl.models.transactions import Payment
from xrpl.asyncio.transaction import autofill, sign, submit_and_wait
from xrpl.utils import xrp_to_drops

XRPL_WS = "wss://xrplcluster.com"

# Configuration (Loaded from environment variables for security)
WALLET_SEED = os.environ.get("XRPL_SEED", "")
DESTINATION = os.environ.get("DESTINATION", "")

if not WALLET_SEED or not DESTINATION:
    print("Error: XRPL_SEED or DESTINATION environment variables are not set.")
    print("Please create a .env file or set them in your environment.")
    exit(1)

BASE_RESERVE = 10        # XRP
FEE_BUFFER = 0.00002    # XRP


async def get_balance(client, address):
    try:
        req = AccountInfo(
            account=address,
            ledger_index="validated",
            strict=True
        )
        resp = await client.request(req)
        
        if resp.is_error():
            # Account not found (actNotFound) or other error
            return 0.0
            
        drops = int(resp.result["account_data"]["Balance"])
        return drops / 1_000_000
    except Exception:
        return 0.0


async def main():
    try:
        # Detect if it's a mnemonic phrase (multiple words) 
        # or a family seed (single string)
        if len(WALLET_SEED.split()) > 1:
            print("Mnemonic phrase detected. Deriving wallet...")
            # Convert mnemonic to entropy for Wallet.from_entropy
            mnemo = Mnemonic("english")
            entropy = mnemo.to_entropy(WALLET_SEED)
            wallet = Wallet.from_entropy(entropy.hex())
        else:
            print("Family seed detected. Initializing wallet...")
            wallet = Wallet.from_seed(WALLET_SEED)
            
    except Exception as e:
        print(f"Error initializing wallet: {e}")
        print("\nNote: Ensure your seed starts with 's' or your mnemonic is 12/24 valid words.")
        return

    async with AsyncWebsocketClient(XRPL_WS) as client:
        balance = await get_balance(client, wallet.address)
        print(f"Listening on {wallet.address}")
        print(f"Current Balance: {balance:.6f} XRP")

        await client.request(
            Subscribe(accounts=[wallet.address])
        )

        async for msg in client:
            if "transaction" not in msg:
                continue

            tx = msg["transaction"]

            if (
                tx.get("TransactionType") == "Payment"
                and tx.get("Destination") == wallet.address
                and msg.get("validated") is True
            ):
                print("Incoming XRP detected")

                balance = await get_balance(client, wallet.address)
                sendable = balance - BASE_RESERVE - FEE_BUFFER

                if sendable <= 0:
                    print("Insufficient balance to sweep")
                    continue

                payment = Payment(
                    account=wallet.address,
                    destination=DESTINATION,
                    amount=xrp_to_drops(sendable)
                )

                prepared = await autofill(payment, client)
                signed = sign(prepared, wallet)
                result = await submit_and_wait(signed, client)

                print(
                    f"Swept {sendable:.6f} XRP | "
                    f"Result: {result.result['meta']['TransactionResult']}"
                )


asyncio.run(main())
