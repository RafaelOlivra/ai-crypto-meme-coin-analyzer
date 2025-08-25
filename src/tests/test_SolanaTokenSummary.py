from services.logger.Logger import _log

from services.SolanaTokenSummary import SolanaTokenSummary

TEST_TOKEN_ADDRESS = "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump" # BILLY
TEST_TOKEN_POOL = "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ" # Raydium
TEST_WALLET_ADDRESS = "GixMsyA2jeAoUEQkF2vZD77DdGGh7FFyW8qsezetyEs3"

## Solana RPC

def test_rpc_invalid_endpoint():
    solana = SolanaTokenSummary(rpc_endpoints=["https://invalid.rpc.endpoint"])
    mint_info = solana._rpc_get_mint_info(TEST_TOKEN_ADDRESS)
    _log("RPC Mint Info (Invalid Endpoint):", mint_info)
    assert mint_info is None

def test_rpc_get_mint_info():
    solana = SolanaTokenSummary()
    mint_info = solana._rpc_get_mint_info(TEST_TOKEN_ADDRESS)
    _log("RPC Mint Info:", mint_info)
    assert isinstance(mint_info, dict)
    assert "mintAuthority" in mint_info
    assert "supply" in mint_info

def test_rpc_get_token_supply():
    solana = SolanaTokenSummary()
    token_supply = solana._rpc_get_token_supply(TEST_TOKEN_ADDRESS)
    _log("RPC Token Supply:", token_supply)
    assert isinstance(token_supply, int)
    assert token_supply > 100

def test_rpc_get_largest_accounts():
    solana = SolanaTokenSummary()
    largest_accounts = solana._rpc_get_largest_accounts(TEST_TOKEN_ADDRESS)
    _log("RPC Largest Accounts:", largest_accounts)
    assert isinstance(largest_accounts, list)
    assert len(largest_accounts) > 0

def test_rpc_get_wallet_age():
    solana = SolanaTokenSummary()
    wallet_age = solana._rpc_estimate_wallet_age(TEST_WALLET_ADDRESS)
    _log("RPC Wallet Age:", wallet_age)
    assert isinstance(wallet_age, int)
    assert wallet_age > 23

## Birdeye

def test_birdeye_get_token_security():
    solana = SolanaTokenSummary()
    security_info = solana._birdeye_get_token_security(TEST_TOKEN_ADDRESS)
    assert isinstance(security_info, dict)
    assert "freezeAuthority" in security_info
    assert "nonTransferable" in security_info
    assert "isTrueToken" in security_info

def test_birdeye_get_wallet_overview():
    solana = SolanaTokenSummary()
    wallet_info = solana._birdeye_get_wallet_overview(TEST_WALLET_ADDRESS)
    _log("Wallet overview:", wallet_info)
    assert isinstance(wallet_info, dict)
    assert "net_worth" in wallet_info

def test_birdeye_get_token_supply():
    solana = SolanaTokenSummary()
    token_supply = solana._birdeye_get_token_supply(TEST_TOKEN_ADDRESS)
    _log("Token supply:", token_supply)
    assert isinstance(token_supply, float)
    assert token_supply > 0

## Solscan

def test_solscan_get_wallet_metadata():
    solana = SolanaTokenSummary()
    metadata = solana._solscan_get_wallet_metadata(TEST_WALLET_ADDRESS)
    _log("Wallet Metadata Solscan:", metadata)
    assert isinstance(metadata, dict)
    assert "account_address" in metadata
    assert "funded_by" in metadata
    assert "active_age" in metadata

def test_solscan_estimate_wallet_age():
    solana = SolanaTokenSummary()
    wallet_age = solana._solscan_estimate_wallet_age(TEST_WALLET_ADDRESS)
    _log("Wallet Age Solscan:", wallet_age)
    assert isinstance(wallet_age, int)
    assert wallet_age > 18

def test_solscan_get_wallet_created_pools():
    solana = SolanaTokenSummary()
    created_pools = solana._solscan_get_wallet_created_pools(TEST_WALLET_ADDRESS)
    _log("Created Pools Solscan:", len(created_pools))
    assert isinstance(created_pools, list)
    assert len(created_pools) >= 1

## Dexscreener

def test_get_dexscreener_token_pair_info():
    solana = SolanaTokenSummary()
    dex_info = solana._dexscreener_get_token_pair_info(
        TEST_TOKEN_ADDRESS,
        TEST_TOKEN_POOL
    )
    assert isinstance(dex_info, dict)
    assert "liquidity" in dex_info
    assert "priceNative" in dex_info
    assert "priceUsd" in dex_info
    assert "volume" in dex_info
## RUG CHECK

def test_rug_check_get_token_info():
    solana = SolanaTokenSummary()
    token = TEST_TOKEN_ADDRESS 
    token_info = solana._rugcheck_get_token_info(token)
    assert isinstance(token_info, dict)
    assert "token" in token_info
    assert "markets" in token_info

def test_rug_check_get_token_risks():
    solana = SolanaTokenSummary()
    token = TEST_TOKEN_ADDRESS
    risks = solana._rugcheck_get_token_risks(token)
    _log("Risks:", risks)
    assert isinstance(risks, list)
    assert "High" in risks[0]

def test_rug_check_get_market_data():
    solana = SolanaTokenSummary()
    mint_address = TEST_TOKEN_ADDRESS
    pair_address = TEST_TOKEN_POOL
    market_data = solana._rugcheck_get_market_data(mint_address, pair_address)
    assert isinstance(market_data, dict)
    assert "pubkey" in market_data
    assert market_data["pubkey"] == pair_address
    assert "lp" in market_data

def test_rug_check_liquidity_locked():
    solana = SolanaTokenSummary()
    mint_address = "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump"
    pair_address = "Fx8KEqbgsipindeuYw8poz8yw77F54sEef2DrBRjKgFB"
    is_locked = solana._rugcheck_is_liquidity_locked(mint_address, pair_address)
    _log("Liquidity Locked:", is_locked)
    assert isinstance(is_locked, bool)
    assert is_locked == True
    
    pair_address = "7CiUuSUH9ajRWLUmTBM7ry5uEVtCHmQxe3hQuSqUpgz1"
    is_locked = solana._rugcheck_is_liquidity_locked(mint_address, pair_address)
    _log("Liquidity Locked:", is_locked)
    assert isinstance(is_locked, bool)
    assert is_locked == False
    
    
## Aggregator

def test_get_token_summary():
    solana = SolanaTokenSummary()
    status = solana.get_token_summary(
        TEST_TOKEN_ADDRESS,
        TEST_TOKEN_POOL
    )
    _log("Token Summary:", status)
    assert isinstance(status, dict)
    assert "rc_risks_desc" in status
    assert "be_freeze_authority" in status