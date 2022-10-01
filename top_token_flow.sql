-- looking for big trades 
with big_trades_with_arb as (
    SELECT
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        trader_a,
        case 
            when trader_b is NULL then trader_a
            else trader_b 
        end as trader_b,
        usd_amount,
        token_a_address,
        token_b_address,
        tx_hash
    FROM dex.trades as d1
    -- set with a parameter
    WHERE usd_amount > {{minimum trade amount in usd}} 
        -- in the last day
        and block_time > now() - interval '{{days}} days'
        -- exclude stablecoin trades
        and not (
            token_a_symbol in ('USDC', 'DAI', 'USDT', 'MIM', 'bb-a-DAI', 'bb-a-USDC', 'bb-a-USDT', 'aUSDC(v2)', 'PAX', 'aDAI', 'aUSDT', 'FRAX', '3Crv', 'BUSD', 'TUSD', 'cUSDT', 'cUSDC', 'cDAI') 
                and token_b_symbol in ('USDC', 'DAI', 'USDT', 'MIM', 'bb-a-DAI', 'bb-a-USDC', 'bb-a-USDT', 'aUSDC(v2)', 'PAX', 'aDAI', 'aUSDT', 'FRAX', '3Crv', 'BUSD', 'TUSD', 'cUSDT', 'cUSDC', 'cDAI')
            or token_a_symbol in ('ETH', 'stETH', 'WETH', 'wstETH') 
                and token_b_symbol in ('ETH', 'stETH', 'WETH', 'wstETH')
            or token_a_symbol in ('WBTC', 'renBTC')
                and token_b_symbol in ('WBTC', 'renBTC')
        )
),

-- exclude arbitrages
big_trades as (
    select 
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        usd_amount,
        1.0 * usd_amount / token_a_amount as price_a,
        1.0 * usd_amount / token_b_amount as price_b,
        trader_a,
        case 
            when trader_b is NULL then trader_a
            else trader_b 
        end as trader_b,
        token_a_address,
        token_b_address,
        tx_hash
    from big_trades_with_arb as tb1
    where token_a_address not in (
        select token_b_address
        from big_trades_with_arb as tb2
        where tb1.block_time = tb2.block_time
    ) 
    or token_b_address not in (
        select token_a_address
        from big_trades_with_arb as tb2
        where tb1.block_time = tb2.block_time
    )
),

buy_flows as (
    select 
        token_b_symbol as token,
        sum(usd_amount) as total
    from big_trades
    group by token_b_symbol
),

sell_flows as (
    select 
        token_a_symbol as token,
        -1 * sum(usd_amount) as total
    from big_trades
    group by token_a_symbol
),

net_flows as (
    select 
        token,
        sum(total) as total
    from (
        select * from buy_flows
        union
        select * from sell_flows
    ) as nf
    group by token
)

select 
    *
from net_flows
where token not in ('WETH', 'USDC', 'USDT', 'WBTC', 'DAI', 'ETH')
ORDER BY total desc
limit 30;
