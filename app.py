import os
import asyncio
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.wallet import Wallet
from xrpl.models.requests import Subscribe, AccountInfo
from xrpl.models.transactions import Payment
from xrpl.transaction import autofill, sign, submit_and_wait
from xrpl.utils import xrp_to_drops

XRPL_WS = "wss://xrplcluster.com"

# Loaded from Replit Secrets
WALLET_SEED = os.environ["XRPL_SEED"]
DESTINATION = os.environ["DESTINATION"]

BASE_RESERVE = 10        # XRP
FEE_BUFFER = 0.00002    # XRP


async def get_balance(client, address):
    req = AccountInfo(
        account=address,
        ledger_index="validated",
        strict=True
    )
    resp = await client.request(req)
    drops = int(resp.result["account_data"]["Balance"])
    return drops / 1_000_000


async def main():
    wallet = Wallet.from_seed(WALLET_SEED)

    async with AsyncWebsocketClient(XRPL_WS) as client:
        print(f"Listening on {wallet.classic_address}")

        await client.request(
            Subscribe(accounts=[wallet.classic_address])
        )

        async for msg in client:
            if "transaction" not in msg:
                continue

            tx = msg["transaction"]

            if (
                tx.get("TransactionType") == "Payment"
                and tx.get("Destination") == wallet.classic_address
                and msg.get("validated") is True
            ):
                print("Incoming XRP detected")

                balance = await get_balance(client, wallet.classic_address)
                sendable = balance - BASE_RESERVE - FEE_BUFFER

                if sendable <= 0:
                    print("Insufficient balance to sweep")
                    continue

                payment = Payment(
                    account=wallet.classic_address,
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
