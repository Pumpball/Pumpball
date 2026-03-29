PUMPBALL

Automated SOL lottery powered by pump.fun creator fees.
Hold $PUMPBALL tokens → they act like lottery tickets → win SOL. Fully on-chain, fully verifiable.

---
How It Works

Every trade on pump.fun generates creator fees in SOL
Every 5 minutes the bot collects those fees and splits them into two prize pools
Minor Reward — a weighted random draw runs frequently, sending the entire minor pot to one lucky holder
Major Reward — a larger pot accumulates over time and pays out to one winner
More tokens = more tickets. No cap. Winners receive SOL directly to their wallet.

---
Eligibility

Hold at least 100,000 $PUMPBALL tokens
Every 100k tokens = 1 lottery ticket
Your wallet is checked on-chain — no registration needed
Burn addresses and the creator wallet are excluded

---
Architecture

Component	Purpose
`lottery_bot.py`	Core bot — fee collection, pot management, draws, payouts
Supabase (PostgreSQL)	Stores pot balances, winner history, draw timers, payout locks
PumpPortal API	Claims creator fees from pump.fun / pump-swap
Helius RPC	Solana RPC calls + token holder snapshots
Jupiter Price API	SOL/USD price for display

---
Crash Safety

The bot uses a payout lock system to prevent double payouts:
Before sending any prize, a pending lock is written to the database
The transaction signature is saved immediately after broadcast
On restart, the bot checks for pending locks and verifies them on-chain
If the tx landed → pot is zeroed, winner recorded
If the tx didn't land → pot is preserved for the next draw

---
Configuration

All settings are controlled via environment variables (`.env` file):
Variable	Default	Description
`COLLECTION_INTERVAL_MINUTES`	`5`	How often to collect fees
`HOURLY_DRAW_MINUTES`	`15`	Minor reward draw interval
`JACKPOT_DRAW_MINUTES`	`1440`	Major reward draw interval (24h)
`MIN_HOLDING`	`100000`	Minimum tokens to qualify
`ENTRIES_PER_TOKENS`	`100000`	Tokens per lottery ticket
`SOL_RESERVE`	`0.05`	SOL kept in wallet for gas
`HOURLY_SPLIT_PCT`	`50`	% of fees to minor pot (rest to major)
`CLAIM_POOL`	`both`	Fee source: `pump`, `pump-swap`, or `both`

---
Transparency

This repo contains the exact bot code running the $PUMPBALL lottery. You can verify:
Fee collection logic and 50/50 pot split
Weighted random winner selection (proportional to holdings)
On-chain payout via native SOL transfer
No hidden fees, no admin withdrawals, no backdoors
Every draw, every winner, every payout is logged to the database and verifiable on-chain via Solscan.

---
Links

Website: (coming soon)
Token: Solscan (link added after launch)
Twitter: (coming soon)

---
Holders don't sell. Holders win.
