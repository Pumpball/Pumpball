"""
Pump.fun SOL Lottery Bot
=========================
Collects creator fees every 5 minutes, splits 50/50 between two pots,
and runs:
  - Hourly draw: sends hourly pot to a weighted-random holder
  - Jackpot draw: sends jackpot pot to a weighted-random holder every 24h

Usage:
    1. Copy .env.example to .env and fill in your values
    2. pip install -r requirements.txt
    3. python lottery_bot.py
"""

import os
import sys
import time
import json
import random
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.message import MessageV0
from solders.hash import Hash
from solders.system_program import transfer as system_transfer, TransferParams
from solders.transaction import VersionedTransaction
from solders.commitment_config import CommitmentLevel
from solders.rpc.requests import SendVersionedTransaction
from solders.rpc.config import RpcSendTransactionConfig

# ============================================
# Logging setup
# ============================================
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log_filename = LOG_DIR / f"lottery_bot_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename, encoding="utf-8"),
    ],
)
logger = logging.getLogger("lottery_bot")

# ============================================
# Load config
# ============================================
load_dotenv()

PRIVATE_KEY          = os.getenv("PRIVATE_KEY", "")
TOKEN_MINT           = os.getenv("TOKEN_MINT", "")
HELIUS_API_KEYS_RAW  = os.getenv("HELIUS_API_KEY", "")
DATABASE_URL         = os.getenv("DATABASE_URL", "")

HELIUS_API_KEYS      = [k.strip() for k in HELIUS_API_KEYS_RAW.split(",") if k.strip()]

COLLECTION_INTERVAL_MINUTES = int(os.getenv("COLLECTION_INTERVAL_MINUTES", "5"))
HOURLY_DRAW_MINUTES         = int(os.getenv("HOURLY_DRAW_MINUTES", "60"))
JACKPOT_DRAW_MINUTES        = int(os.getenv("JACKPOT_DRAW_MINUTES", "1440"))   # 24h
TOKEN_DECIMALS              = int(os.getenv("TOKEN_DECIMALS", "6"))
MIN_HOLDING                 = int(os.getenv("MIN_HOLDING", "100000"))           # 100k tokens
ENTRIES_PER_TOKENS          = int(os.getenv("ENTRIES_PER_TOKENS", "100000"))    # 1 entry per 100k
SOL_RESERVE                 = float(os.getenv("SOL_RESERVE", "0.05"))
CLAIM_POOL                  = os.getenv("CLAIM_POOL", "both").lower()
HOURLY_SPLIT_PCT            = int(os.getenv("HOURLY_SPLIT_PCT", "50"))     # % of fees to hourly pot

MIN_HOLDING_RAW  = MIN_HOLDING * (10 ** TOKEN_DECIMALS)
ENTRIES_PER_RAW  = ENTRIES_PER_TOKENS * (10 ** TOKEN_DECIMALS)

# Constants
LAMPORTS_PER_SOL    = 1_000_000_000
SOL_RESERVE_LAMPORTS = int(SOL_RESERVE * LAMPORTS_PER_SOL)
SOL_MINT            = "So11111111111111111111111111111111111111112"
BURN_ADDRESSES      = {
    "11111111111111111111111111111111",
    "1111111111111111111111111111111",
    "1nc1nerator11111111111111111111111111111111",
}
HELIUS_RPC_BASE     = "https://mainnet.helius-rpc.com/?api-key="
PUMPPORTAL_LOCAL_URL = "https://pumpportal.fun/api/trade-local"
GAS_PER_TX_LAMPORTS = 1_000_000   # 0.001 SOL

_helius_key_index = 0


def get_helius_rpc_url() -> str:
    global _helius_key_index
    url = f"{HELIUS_RPC_BASE}{HELIUS_API_KEYS[_helius_key_index % len(HELIUS_API_KEYS)]}"
    _helius_key_index += 1
    return url


# ============================================
# Config validation
# ============================================

def validate_config():
    errors = []
    if not PRIVATE_KEY or PRIVATE_KEY == "your_base58_private_key_here":
        errors.append("PRIVATE_KEY is not set")
    else:
        # Validate the key can actually be loaded — catches garbage values early
        try:
            Keypair.from_base58_string(PRIVATE_KEY)
        except Exception as e:
            errors.append(f"PRIVATE_KEY is invalid (cannot parse keypair): {e}")
    if not TOKEN_MINT or TOKEN_MINT == "your_token_ca_here":
        errors.append("TOKEN_MINT is not set")
    else:
        try:
            Pubkey.from_string(TOKEN_MINT)
        except Exception:
            errors.append("TOKEN_MINT is not a valid Solana address")
    if not HELIUS_API_KEYS or HELIUS_API_KEYS == ["your_helius_api_key_here"]:
        errors.append("HELIUS_API_KEY is not set")
    if not DATABASE_URL or DATABASE_URL == "your_supabase_connection_string_here":
        errors.append("DATABASE_URL is not set (required for lottery bot)")
    if CLAIM_POOL not in ("pump", "pump-swap", "both"):
        errors.append(f"CLAIM_POOL must be 'pump', 'pump-swap', or 'both' (got '{CLAIM_POOL}')")
    if not (1 <= HOURLY_SPLIT_PCT <= 99):
        errors.append(f"HOURLY_SPLIT_PCT must be 1-99 (got {HOURLY_SPLIT_PCT})")
    if SOL_RESERVE < 0.01:
        errors.append(f"SOL_RESERVE must be at least 0.01 SOL (got {SOL_RESERVE})")
    if errors:
        for e in errors:
            logger.error(f"Config error: {e}")
        sys.exit(1)


def get_keypair() -> Keypair:
    return Keypair.from_base58_string(PRIVATE_KEY)


# ============================================
# Database helpers
# ============================================

def db_connect():
    return psycopg2.connect(DATABASE_URL)


def db_get_pots() -> dict:
    """Fetch current pot balances. Returns dict with all pot fields."""
    with db_connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM lottery_pots WHERE id = 1")
            row = cur.fetchone()
            if not row:
                raise Exception("lottery_pots row not found — did you run init_db.sql?")
            return dict(row)


def db_add_to_pots(hourly_lamports: int, jackpot_lamports: int, total_collected: int,
                   hourly_after: int, jackpot_after: int):
    """
    Atomically add to both pot balances and log the collection.
    Uses a single transaction so it's all-or-nothing.
    """
    now = datetime.now(timezone.utc)
    with db_connect() as conn:
        with conn.cursor() as cur:
            # Update pot balances atomically
            cur.execute("""
                UPDATE lottery_pots
                SET hourly_pot_lamports  = hourly_pot_lamports  + %s,
                    jackpot_pot_lamports = jackpot_pot_lamports + %s,
                    total_collected_lamports = total_collected_lamports + %s,
                    last_collection_at   = %s,
                    last_updated_at      = %s
                WHERE id = 1
            """, (hourly_lamports, jackpot_lamports, total_collected, now, now))

            # Audit log
            cur.execute("""
                INSERT INTO collection_log
                    (collected_lamports, hourly_share_lamports, jackpot_share_lamports,
                     hourly_pot_after, jackpot_pot_after, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (total_collected, hourly_lamports, jackpot_lamports,
                  hourly_after, jackpot_after, now))
        conn.commit()


def db_update_draw_times(next_hourly: datetime, next_jackpot: datetime):
    """Update next draw timestamps in DB (for frontend countdown)."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE lottery_pots
                SET next_hourly_draw_at  = %s,
                    next_jackpot_draw_at = %s,
                    last_updated_at      = %s
                WHERE id = 1
            """, (next_hourly, next_jackpot, datetime.now(timezone.utc)))
        conn.commit()


def db_create_payout_lock(draw_type: str, amount_lamports: int, winner: str) -> int:
    """
    Insert a pending payout lock before sending any transaction.
    Returns the lock row id.
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO payout_locks (draw_type, status, amount_lamports, winner_wallet)
                VALUES (%s, 'pending', %s, %s)
                RETURNING id
            """, (draw_type, amount_lamports, winner))
            lock_id = cur.fetchone()[0]
        conn.commit()
    return lock_id


def db_confirm_payout_lock(lock_id: int, signature: str):
    """Mark payout lock as confirmed after on-chain success."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE payout_locks
                SET status = 'confirmed', signature = %s, confirmed_at = %s
                WHERE id = %s
            """, (signature, datetime.now(timezone.utc), lock_id))
        conn.commit()


def db_update_lock_signature(lock_id: int, signature: str):
    """
    Persist the tx signature to the lock row IMMEDIATELY after send,
    before waiting for confirmation. This way crash recovery can check
    if the tx landed even if we crash before confirming.
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE payout_locks
                SET signature = %s
                WHERE id = %s
            """, (signature, lock_id))
        conn.commit()


def db_fail_payout_lock(lock_id: int):
    """Mark payout lock as failed."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE payout_locks
                SET status = 'failed'
                WHERE id = %s
            """, (lock_id,))
        conn.commit()


def db_zero_pot(draw_type: str, draw_count_field: str):
    """
    Zero out a pot after successful payout.
    Increments the draw counter atomically.
    """
    # Whitelist both field names to prevent any risk of bad values
    VALID_POT_FIELDS = {
        "hourly":  ("hourly_pot_lamports",  "hourly_draws_count"),
        "jackpot": ("jackpot_pot_lamports", "jackpot_draws_count"),
    }
    if draw_type not in VALID_POT_FIELDS:
        raise ValueError(f"db_zero_pot: invalid draw_type '{draw_type}'")
    pot_field, count_field = VALID_POT_FIELDS[draw_type]

    now = datetime.now(timezone.utc)
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE lottery_pots
                SET {pot_field} = 0,
                    {count_field} = {count_field} + 1,
                    last_updated_at = %s
                WHERE id = 1
            """, (now,))
        conn.commit()


def db_record_winner(draw_type: str, winner: str, amount_lamports: int,
                     amount_sol: float, amount_usd: float | None,
                     sol_price: float | None, signature: str, draw_number: int):
    """Record a winner in the winners table."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO lottery_winners
                    (draw_type, winner_wallet, amount_lamports, amount_sol,
                     amount_usd, sol_price_usd, signature, draw_number, drawn_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                draw_type, winner, amount_lamports, amount_sol,
                amount_usd, sol_price, signature, draw_number,
                datetime.now(timezone.utc)
            ))
        conn.commit()


def db_check_pending_locks() -> list[dict]:
    """On startup, find any pending locks from a previous crashed run."""
    with db_connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM payout_locks
                WHERE status = 'pending'
                  AND created_at > NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC
            """)
            return [dict(r) for r in cur.fetchall()]


# ============================================
# Solana RPC helpers
# ============================================

def rpc_request(method: str, params: list | dict) -> dict:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    resp = requests.post(
        get_helius_rpc_url(),
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise Exception(f"RPC error: {data['error']}")
    return data.get("result", {})


def get_sol_balance(pubkey: str) -> int:
    """Return SOL balance in lamports (integer, no float errors)."""
    result = rpc_request("getBalance", [pubkey])
    return result.get("value", 0)


def send_signed_transaction(signed_tx: VersionedTransaction) -> str:
    commitment = CommitmentLevel.Confirmed
    config = RpcSendTransactionConfig(preflight_commitment=commitment)
    payload_rpc = SendVersionedTransaction(signed_tx, config)

    resp = requests.post(
        get_helius_rpc_url(),
        headers={"Content-Type": "application/json"},
        data=payload_rpc.to_json(),
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise Exception(f"Transaction error: {data['error']}")
    return data.get("result", "")


def confirm_transaction(signature: str, max_retries: int = 30, delay: float = 2.0) -> bool:
    for i in range(max_retries):
        try:
            result = rpc_request(
                "getSignatureStatuses",
                [[signature], {"searchTransactionHistory": True}],
            )
            statuses = result.get("value", [])
            if statuses and statuses[0]:
                status = statuses[0]
                if status.get("err"):
                    logger.error(f"Transaction failed on-chain: {status['err']}")
                    return False
                confirmation = status.get("confirmationStatus", "")
                if confirmation in ("confirmed", "finalized"):
                    return True
        except Exception as e:
            logger.debug(f"Confirmation check {i+1} failed: {e}")
        time.sleep(delay)
    return False


def check_signature_exists(signature: str) -> bool:
    """Check if a tx signature already landed on-chain (crash recovery)."""
    try:
        result = rpc_request(
            "getSignatureStatuses",
            [[signature], {"searchTransactionHistory": True}],
        )
        statuses = result.get("value", [])
        if statuses and statuses[0]:
            return not statuses[0].get("err")
        return False
    except Exception:
        return False


# ============================================
# Fee claiming
# ============================================

def claim_creator_fees(keypair: Keypair, pool: str) -> str | None:
    logger.info(f"Claiming creator fees from pool: {pool}")

    request_data = {
        "publicKey": str(keypair.pubkey()),
        "action": "collectCreatorFee",
        "priorityFee": 0.001,
    }

    if pool == "pump-swap":
        request_data["pool"] = "meteora-dbc"
        request_data["mint"] = TOKEN_MINT
    else:
        request_data["pool"] = "pump"

    try:
        resp = requests.post(PUMPPORTAL_LOCAL_URL, data=request_data, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"PumpPortal {pool} returned {resp.status_code}: {resp.text[:200]}")
            return None

        tx_bytes = resp.content
        tx = VersionedTransaction(
            VersionedTransaction.from_bytes(tx_bytes).message,
            [keypair],
        )
        signature = send_signed_transaction(tx)
        logger.info(f"Fee claim ({pool}) tx: https://solscan.io/tx/{signature}")

        if confirm_transaction(signature):
            logger.info(f"Fee claim ({pool}) confirmed")
            return signature
        else:
            logger.warning(f"Fee claim ({pool}) not confirmed within timeout")
            return signature

    except Exception as e:
        logger.error(f"Failed to claim fees from {pool}: {e}")
        return None


def claim_all_fees(keypair: Keypair) -> list[str]:
    signatures = []
    pools = ["pump", "pump-swap"] if CLAIM_POOL == "both" else [CLAIM_POOL]
    for pool in pools:
        sig = claim_creator_fees(keypair, pool)
        if sig:
            signatures.append(sig)
        if len(pools) > 1:
            time.sleep(3)
    return signatures


# ============================================
# SOL price
# ============================================

# Optional Jupiter API key for price lookups (free tier at portal.jup.ag)
JUPITER_API_KEY = os.getenv("JUPITER_API_KEY", "")

# Cache: avoid hammering price APIs when draw + collection fire close together
_sol_price_cache: dict = {"price": None, "fetched_at": 0.0}
SOL_PRICE_CACHE_SECONDS = 30


def get_sol_price_usd() -> float | None:
    """
    Fetch live SOL price in USD.
    Primary: Jupiter Price API V3 (free key from portal.jup.ag)
    Fallback: DexScreener (no key needed)
    Caches result for 30s to avoid rate limits when multiple calls happen close together.
    """
    now = time.time()
    if _sol_price_cache["price"] and (now - _sol_price_cache["fetched_at"]) < SOL_PRICE_CACHE_SECONDS:
        return _sol_price_cache["price"]

    price = _fetch_sol_price_jupiter()
    if price is None:
        price = _fetch_sol_price_dexscreener()

    if price:
        _sol_price_cache["price"] = price
        _sol_price_cache["fetched_at"] = now

    return price


def _fetch_sol_price_jupiter() -> float | None:
    """Jupiter Price API V3 — requires free API key from portal.jup.ag"""
    if not JUPITER_API_KEY:
        return None
    try:
        headers = {"x-api-key": JUPITER_API_KEY}
        resp = requests.get(
            "https://api.jup.ag/price/v3",
            params={"ids": SOL_MINT},
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return float(data[SOL_MINT]["usdPrice"])
    except Exception as e:
        logger.warning(f"Jupiter V3 price fetch failed: {e}")
        return None


def _fetch_sol_price_dexscreener() -> float | None:
    """DexScreener API — no key needed, generous rate limits."""
    try:
        resp = requests.get(
            "https://api.dexscreener.com/latest/dex/tokens/" + SOL_MINT,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs", [])
        if pairs:
            return float(pairs[0]["priceUsd"])
        return None
    except Exception as e:
        logger.warning(f"DexScreener price fallback failed: {e}")
        return None


# ============================================
# Holder snapshot
# ============================================

def get_eligible_holders(mint: str, my_pubkey: str) -> list[dict]:
    """
    Fetch all token holders and return eligible ones with entry counts.
    Eligibility: >= MIN_HOLDING_RAW tokens, not a burn address, not our wallet.
    No whale cap — higher holdings = more entries.
    """
    logger.info(f"Fetching token holders for {mint[:8]}...")

    holders_raw: dict[str, int] = {}
    cursor = None
    page = 0

    while True:
        page += 1
        params = {"limit": 1000, "mint": mint}
        if cursor:
            params["cursor"] = cursor

        try:
            payload = {
                "jsonrpc": "2.0",
                "id": "holder-scan",
                "method": "getTokenAccounts",
                "params": params,
            }

            # Retry up to 3 times per page to handle transient Helius errors
            resp = None
            for attempt in range(3):
                try:
                    resp = requests.post(
                        get_helius_rpc_url(),
                        headers={"Content-Type": "application/json"},
                        json=payload,
                        timeout=30,
                    )
                    resp.raise_for_status()
                    break
                except Exception as retry_err:
                    if attempt < 2:
                        logger.warning(f"Holder fetch page {page} attempt {attempt+1} failed: {retry_err}, retrying...")
                        time.sleep(2)
                    else:
                        raise

            data = resp.json()
            result = data.get("result", {})
            token_accounts = result.get("token_accounts", [])

            if not token_accounts:
                break

            for acc in token_accounts:
                owner = acc.get("owner", "")
                amount = int(acc.get("amount", 0))
                if owner and amount > 0:
                    holders_raw[owner] = holders_raw.get(owner, 0) + amount

            cursor = result.get("cursor")
            if not cursor:
                break

            time.sleep(0.3)

        except Exception as e:
            logger.error(f"Error fetching holders page {page}: {e}")
            break

    logger.info(f"Total unique holders found: {len(holders_raw)}")

    eligible = []
    excluded = {"burn": 0, "self": 0, "dust": 0}

    for owner, amount in holders_raw.items():
        if owner in BURN_ADDRESSES:
            excluded["burn"] += 1
            continue
        if owner == my_pubkey:
            excluded["self"] += 1
            continue
        if amount < MIN_HOLDING_RAW:
            excluded["dust"] += 1
            continue

        # Calculate entries: floor(amount / ENTRIES_PER_RAW), minimum 1
        entries = max(1, amount // ENTRIES_PER_RAW)
        eligible.append({"owner": owner, "amount": amount, "entries": entries})

    total_entries = sum(h["entries"] for h in eligible)
    logger.info(
        f"Eligible: {len(eligible)} holders, {total_entries} total entries | "
        f"Excluded: burn={excluded['burn']}, self={excluded['self']}, dust={excluded['dust']}"
    )
    return eligible


def pick_weighted_winner(eligible: list[dict]) -> str | None:
    """Pick a winner weighted by entry count."""
    if not eligible:
        return None

    total_entries = sum(h["entries"] for h in eligible)
    if total_entries == 0:
        return None

    roll = random.randint(0, total_entries - 1)
    cumulative = 0
    for h in eligible:
        cumulative += h["entries"]
        if roll < cumulative:
            return h["owner"]

    # Fallback (shouldn't happen)
    return eligible[-1]["owner"]


# ============================================
# SOL transfer (native, no token swap needed)
# ============================================

def send_sol(keypair: Keypair, recipient: str, amount_lamports: int) -> str | None:
    """
    Send native SOL to a recipient.
    This is a simple system program transfer — no token involved.
    """
    if amount_lamports <= 0:
        logger.warning("send_sol called with 0 lamports — skipping")
        return None

    try:
        recipient_pubkey = Pubkey.from_string(recipient)
        sender = keypair.pubkey()

        transfer_ix = system_transfer(
            TransferParams(
                from_pubkey=sender,
                to_pubkey=recipient_pubkey,
                lamports=amount_lamports,
            )
        )

        blockhash_result = rpc_request("getLatestBlockhash", [{"commitment": "confirmed"}])
        recent_blockhash = Hash.from_string(blockhash_result["value"]["blockhash"])

        msg = MessageV0.try_compile(
            payer=sender,
            instructions=[transfer_ix],
            address_lookup_table_accounts=[],
            recent_blockhash=recent_blockhash,
        )
        tx = VersionedTransaction(msg, [keypair])
        signature = send_signed_transaction(tx)

        logger.info(f"SOL transfer tx: https://solscan.io/tx/{signature}")
        return signature

    except Exception as e:
        logger.error(f"SOL transfer failed: {e}")
        return None


# ============================================
# Draw execution (with crash safety)
# ============================================

def execute_draw(keypair: Keypair, draw_type: str, pot_lamports: int,
                 draw_count_field: str) -> bool:
    """
    Execute a lottery draw:
    1. Check wallet has enough SOL for gas
    2. Pick winner
    3. Create payout lock in DB
    4. Send SOL on-chain
    5. Persist signature to lock immediately (crash safety)
    6. Confirm on-chain
    7. Zero pot in DB
    8. Record winner in DB
    9. Confirm lock

    Returns True if draw completed successfully.
    """
    my_pubkey = str(keypair.pubkey())

    logger.info(f"{'=' * 50}")
    logger.info(f"DRAW: {draw_type.upper()} | Pot: {pot_lamports} lamports "
                f"({pot_lamports / LAMPORTS_PER_SOL:.6f} SOL)")
    logger.info(f"{'=' * 50}")

    # Reserve enough gas for the payout tx
    gas_reserve = GAS_PER_TX_LAMPORTS * 2
    payout_lamports = pot_lamports - gas_reserve

    if payout_lamports <= 0:
        # Pot is too small even for gas — leave it and let it accumulate
        logger.warning(
            f"{draw_type} pot ({pot_lamports} lamports) is smaller than gas reserve "
            f"({gas_reserve} lamports). Skipping payout, pot carries over."
        )
        return False

    # Check wallet has enough SOL to cover gas + reserve
    wallet_balance = get_sol_balance(my_pubkey)
    gas_needed = GAS_PER_TX_LAMPORTS  # actual on-chain gas comes from wallet
    if wallet_balance < gas_needed + SOL_RESERVE_LAMPORTS:
        logger.error(
            f"Wallet balance ({wallet_balance} lamports) too low for draw. "
            f"Need at least {gas_needed + SOL_RESERVE_LAMPORTS} lamports "
            f"(gas + {SOL_RESERVE} SOL reserve). Skipping draw, pot carries over."
        )
        return False

    # Get eligible holders
    eligible = get_eligible_holders(TOKEN_MINT, my_pubkey)
    if not eligible:
        logger.warning(f"No eligible holders for {draw_type} draw. Pot carries over.")
        return False

    # Pick winner
    winner = pick_weighted_winner(eligible)
    if not winner:
        logger.warning("Winner selection failed. Pot carries over.")
        return False

    winner_entries = next((h["entries"] for h in eligible if h["owner"] == winner), 0)
    total_entries = sum(h["entries"] for h in eligible)
    logger.info(
        f"Winner: {winner} | "
        f"Entries: {winner_entries}/{total_entries} "
        f"({winner_entries/total_entries*100:.2f}% chance)"
    )

    # Get current SOL price for USD display
    sol_price = get_sol_price_usd()
    amount_sol = payout_lamports / LAMPORTS_PER_SOL
    amount_usd = round(amount_sol * sol_price, 2) if sol_price else None

    # Get draw number
    pots = db_get_pots()
    draw_number = pots.get(draw_count_field, 0) + 1

    # --- CRITICAL SECTION: payout lock ---
    lock_id = db_create_payout_lock(draw_type, payout_lamports, winner)
    logger.info(f"Payout lock created: id={lock_id}")

    signature = None
    try:
        # Send SOL
        signature = send_sol(keypair, winner, payout_lamports)

        if not signature:
            logger.error(f"{draw_type} draw: SOL transfer returned no signature")
            db_fail_payout_lock(lock_id)
            return False

        # CRITICAL: persist signature to lock row IMMEDIATELY
        # so crash recovery can find and verify it on-chain
        db_update_lock_signature(lock_id, signature)
        logger.info(f"Signature persisted to lock: {signature}")

        # Wait for on-chain confirmation
        logger.info("Waiting for on-chain confirmation...")
        confirmed = confirm_transaction(signature, max_retries=40, delay=2.0)

        if not confirmed:
            # Check one more time with history search
            confirmed = check_signature_exists(signature)

        if not confirmed:
            logger.error(
                f"{draw_type} draw tx not confirmed: {signature} "
                f"— pot NOT zeroed, lock stays pending for manual review"
            )
            # DON'T fail the lock here — leave it pending so crash recovery
            # can re-check later. The tx might still land.
            return False

        # --- On-chain confirmed: now update DB atomically ---
        logger.info(f"{draw_type} draw confirmed on-chain!")

        # Zero the pot
        db_zero_pot(draw_type, draw_count_field)

        # Record winner
        db_record_winner(
            draw_type=draw_type,
            winner=winner,
            amount_lamports=payout_lamports,
            amount_sol=amount_sol,
            amount_usd=amount_usd,
            sol_price=sol_price,
            signature=signature,
            draw_number=draw_number,
        )

        # Confirm lock
        db_confirm_payout_lock(lock_id, signature)

        logger.info(
            f"{draw_type.upper()} DRAW COMPLETE | "
            f"Winner: {winner} | "
            f"Amount: {amount_sol:.6f} SOL"
            + (f" (${amount_usd:.2f})" if amount_usd else "")
            + f" | TX: https://solscan.io/tx/{signature}"
        )
        return True

    except Exception as e:
        logger.error(f"{draw_type} draw failed with exception: {e}", exc_info=True)
        if lock_id:
            db_fail_payout_lock(lock_id)
        return False


# ============================================
# Fee collection + pot split
# ============================================

def run_collection(keypair: Keypair, _balance_unused: int) -> int:
    """
    Claim fees, measure how much SOL arrived, split into pots.
    Takes a fresh balance snapshot BEFORE claiming to avoid drift
    from payouts that happened between cycles.
    Returns new balance in lamports.
    """
    my_pubkey = str(keypair.pubkey())

    # FRESH balance snapshot right before claiming — not carried from last cycle
    balance_before = get_sol_balance(my_pubkey)

    logger.info("--- Collection cycle: claiming fees ---")
    claim_sigs = claim_all_fees(keypair)

    if claim_sigs:
        logger.info("Waiting 5s for claims to settle...")
        time.sleep(5)

    balance_after = get_sol_balance(my_pubkey)
    collected = balance_after - balance_before

    if collected <= 0:
        logger.info(f"No new SOL collected this cycle (balance: {balance_after} lamports)")
        return balance_after

    logger.info(f"Collected: {collected} lamports ({collected / LAMPORTS_PER_SOL:.6f} SOL)")

    # Split based on configurable HOURLY_SPLIT_PCT (default 50%)
    hourly_share  = (collected * HOURLY_SPLIT_PCT) // 100
    jackpot_share = collected - hourly_share   # remainder to jackpot (no rounding loss)

    logger.info(
        f"Split ({HOURLY_SPLIT_PCT}/{100 - HOURLY_SPLIT_PCT}): "
        f"hourly={hourly_share} lamports, jackpot={jackpot_share} lamports"
    )

    # Read current pots to compute after values for audit log
    pots = db_get_pots()
    hourly_after  = pots["hourly_pot_lamports"] + hourly_share
    jackpot_after = pots["jackpot_pot_lamports"] + jackpot_share

    # Atomically update both pots and log
    db_add_to_pots(
        hourly_lamports=hourly_share,
        jackpot_lamports=jackpot_share,
        total_collected=collected,
        hourly_after=hourly_after,
        jackpot_after=jackpot_after,
    )

    logger.info(
        f"Pots updated | Hourly: {hourly_after} lamports | "
        f"Jackpot: {jackpot_after} lamports"
    )

    return balance_after


# ============================================
# Crash recovery on startup
# ============================================

def recover_pending_locks(keypair: Keypair):
    """
    On startup, check for any pending payout locks from a previous crash.
    If the tx landed on-chain: record the winner, zero the pot, confirm the lock.
    If not: fail the lock so the pot is preserved for the next draw.
    """
    pending = db_check_pending_locks()
    if not pending:
        return

    logger.warning(f"Found {len(pending)} pending payout lock(s) from previous run — recovering...")

    sol_price = get_sol_price_usd()

    for lock in pending:
        lock_id         = lock["id"]
        draw_type       = lock["draw_type"]
        signature       = lock.get("signature")
        amount_lamports = lock["amount_lamports"]
        winner_wallet   = lock["winner_wallet"]

        if signature:
            logger.info(f"Checking lock {lock_id} ({draw_type}): tx {signature}")
            landed = check_signature_exists(signature)
            if landed:
                logger.info(f"  TX landed! Recording winner, zeroing pot.")
                draw_count_field = "hourly_draws_count" if draw_type == "hourly" else "jackpot_draws_count"

                # Get draw number from current DB state before zeroing
                try:
                    pots = db_get_pots()
                    draw_number = pots.get(draw_count_field, 0) + 1
                except Exception:
                    draw_number = 0

                amount_sol = amount_lamports / LAMPORTS_PER_SOL
                amount_usd = round(amount_sol * sol_price, 2) if sol_price else None

                # Record winner (may have been missed due to crash)
                try:
                    db_record_winner(
                        draw_type=draw_type,
                        winner=winner_wallet,
                        amount_lamports=amount_lamports,
                        amount_sol=amount_sol,
                        amount_usd=amount_usd,
                        sol_price=sol_price,
                        signature=signature,
                        draw_number=draw_number,
                    )
                except Exception as e:
                    # May already be recorded if only the lock update crashed — that's fine
                    logger.warning(f"  Winner record insert skipped (may already exist): {e}")

                # Zero the pot
                db_zero_pot(draw_type, draw_count_field)
                db_confirm_payout_lock(lock_id, signature)
                logger.info(f"  Recovery complete for lock {lock_id}")
            else:
                logger.warning(f"  TX did NOT land. Failing lock — pot preserved.")
                db_fail_payout_lock(lock_id)
        else:
            # No signature means we crashed before even sending — pot is safe
            logger.warning(f"  Lock {lock_id} has no signature (crashed before send) — failing lock")
            db_fail_payout_lock(lock_id)


# ============================================
# Main loop
# ============================================

def main():
    logger.info("=" * 60)
    logger.info("PUMP.FUN SOL LOTTERY BOT")
    logger.info("=" * 60)

    validate_config()
    keypair = get_keypair()
    my_pubkey = str(keypair.pubkey())

    logger.info(f"Wallet:              {my_pubkey}")
    logger.info(f"Token:               {TOKEN_MINT}")
    logger.info(f"Helius keys:         {len(HELIUS_API_KEYS)}")
    logger.info(f"Collection interval: {COLLECTION_INTERVAL_MINUTES} min")
    logger.info(f"Hourly draw every:   {HOURLY_DRAW_MINUTES} min")
    logger.info(f"Jackpot draw every:  {JACKPOT_DRAW_MINUTES} min")
    logger.info(f"Pot split:           {HOURLY_SPLIT_PCT}% hourly / {100 - HOURLY_SPLIT_PCT}% jackpot")
    logger.info(f"Min holding:         {MIN_HOLDING:,} tokens")
    logger.info(f"Entries per:         {ENTRIES_PER_TOKENS:,} tokens")
    logger.info(f"SOL reserve:         {SOL_RESERVE} SOL")
    logger.info(f"Claim pool:          {CLAIM_POOL}")
    logger.info("")

    # Crash recovery
    recover_pending_locks(keypair)

    # Restore draw timers from DB if they're still in the future,
    # otherwise reset from now
    now = datetime.now(timezone.utc)
    pots = db_get_pots()

    saved_hourly = pots.get("next_hourly_draw_at")
    saved_jackpot = pots.get("next_jackpot_draw_at")

    if saved_hourly and saved_hourly > now:
        next_hourly_draw = saved_hourly
        logger.info(f"Restored hourly draw timer from DB: {next_hourly_draw.isoformat()}")
    else:
        next_hourly_draw = now + timedelta(minutes=HOURLY_DRAW_MINUTES)
        logger.info(f"Hourly timer expired or missing — reset: {next_hourly_draw.isoformat()}")

    if saved_jackpot and saved_jackpot > now:
        next_jackpot_draw = saved_jackpot
        logger.info(f"Restored jackpot draw timer from DB: {next_jackpot_draw.isoformat()}")
    else:
        next_jackpot_draw = now + timedelta(minutes=JACKPOT_DRAW_MINUTES)
        logger.info(f"Jackpot timer expired or missing — reset: {next_jackpot_draw.isoformat()}")

    # Persist draw times to DB for frontend countdown
    db_update_draw_times(next_hourly_draw, next_jackpot_draw)

    # Initial balance snapshot
    current_balance = get_sol_balance(my_pubkey)
    logger.info(f"Current SOL balance: {current_balance / LAMPORTS_PER_SOL:.6f}")
    logger.info("")

    # Show current pots
    logger.info(f"Current pots:")
    logger.info(f"  Hourly:  {pots['hourly_pot_lamports']} lamports")
    logger.info(f"  Jackpot: {pots['jackpot_pot_lamports']} lamports")
    logger.info("")
    logger.info(f"Starting — first collection in {COLLECTION_INTERVAL_MINUTES} min")
    logger.info("Press Ctrl+C to stop.\n")

    collection_interval = COLLECTION_INTERVAL_MINUTES * 60

    try:
        while True:
            time.sleep(collection_interval)

            now = datetime.now(timezone.utc)
            logger.info(f"\n[{now.isoformat()}] Running collection cycle...")

            try:
                current_balance = run_collection(keypair, current_balance)
            except Exception as e:
                logger.error(f"Collection failed: {e}", exc_info=True)
                # Refresh balance for next cycle
                try:
                    current_balance = get_sol_balance(my_pubkey)
                except Exception:
                    pass

            # Check if hourly draw is due
            now = datetime.now(timezone.utc)
            if now >= next_hourly_draw:
                try:
                    pots = db_get_pots()
                    pot = pots["hourly_pot_lamports"]

                    if pot > 0:
                        logger.info(f"Hourly draw time! Pot: {pot} lamports")
                        success = execute_draw(
                            keypair, "hourly", pot, "hourly_draws_count"
                        )
                        if success:
                            # Refresh balance after payout
                            current_balance = get_sol_balance(my_pubkey)
                    else:
                        logger.info("Hourly draw time — pot is empty, skipping")

                except Exception as e:
                    logger.error(f"Hourly draw failed: {e}", exc_info=True)

                # Schedule next hourly draw — advance past now in case multiple were missed
                while next_hourly_draw <= now:
                    next_hourly_draw = next_hourly_draw + timedelta(minutes=HOURLY_DRAW_MINUTES)
                db_update_draw_times(next_hourly_draw, next_jackpot_draw)
                logger.info(f"Next hourly draw: {next_hourly_draw.isoformat()}")

            # Re-read now — hourly draw may have taken several minutes
            now = datetime.now(timezone.utc)

            # Check if jackpot draw is due
            if now >= next_jackpot_draw:
                try:
                    pots = db_get_pots()
                    pot = pots["jackpot_pot_lamports"]

                    if pot > 0:
                        logger.info(f"JACKPOT draw time! Pot: {pot} lamports")
                        success = execute_draw(
                            keypair, "jackpot", pot, "jackpot_draws_count"
                        )
                        if success:
                            current_balance = get_sol_balance(my_pubkey)
                    else:
                        logger.info("Jackpot draw time — pot is empty, skipping")

                except Exception as e:
                    logger.error(f"Jackpot draw failed: {e}", exc_info=True)

                # Schedule next jackpot draw — advance past now in case multiple were missed
                while next_jackpot_draw <= now:
                    next_jackpot_draw = next_jackpot_draw + timedelta(minutes=JACKPOT_DRAW_MINUTES)
                db_update_draw_times(next_hourly_draw, next_jackpot_draw)
                logger.info(f"Next jackpot draw: {next_jackpot_draw.isoformat()}")

    except KeyboardInterrupt:
        logger.info("\nBot stopped by user. Goodbye!")
        sys.exit(0)


if __name__ == "__main__":
    main()
