# Decibel Perpetual Connector

## Overview

The Decibel Perpetual connector allows Hummingbot to trade on the Decibel perpetual derivatives exchange.

## Features

- ✅ REST API integration for trading
- ✅ WebSocket integration for real-time data
- ✅ Support for limit and market orders
- ✅ Position management (one-way and hedge modes)
- ✅ Real-time order book updates
- ✅ Account and position tracking
- ✅ Funding rate tracking

## Supported Order Types

- **Limit Orders:** Buy or sell at a specified price
- **Market Orders:** Buy or sell at the best available price

## Position Modes

### One-Way Mode
- Single position per trading pair
- Either long or short, not both
- Best for simple trading strategies

### Hedge Mode
- Multiple positions per trading pair
- Can hold both long and short positions simultaneously
- Best for advanced strategies requiring hedging

## Trading Pairs

The connector supports all perpetual contracts listed on Decibel exchange:
- BTC-USDT
- ETH-USDT
- And more...

## Configuration

Create a configuration file at `conf/decibel_perpetual.yml`:

```yaml
decibel_perpetual:
  api_key: "your_api_key"
  api_secret: "your_secret_key"
  passphrase: "your_passphrase"  # Optional
  trading_pairs:
    - "BTC-USDT"
  position_mode: "one_way"
  trading_required: true
```

## API Permissions

Required API key permissions:
- **Read:** View account information, orders, and positions
- **Trade:** Place and cancel orders
- **Withdraw:** Not required for trading

## Fees

| Fee Type | Rate |
|----------|------|
| Maker | 0.02% |
| Taker | 0.05% |

## Rate Limits

The connector implements rate limiting to comply with API limits:

| Endpoint | Limit | Time Window |
|----------|-------|-------------|
| REST Public | 100 requests | 60 seconds |
| REST Private | 50 requests | 60 seconds |
| WebSocket Public | 100 messages | 60 seconds |
| WebSocket Private | 50 messages | 60 seconds |

## Getting Started

1. **Create a Decibel Account**
   - Visit https://decibel.exchange
   - Complete KYC verification

2. **Generate API Keys**
   - Go to API Management in your account
   - Create new API key
   - Set permissions (Read + Trade)
   - Save your API key, secret, and passphrase

3. **Configure Hummingbot**
   - Add credentials to `conf/decibel_perpetual.yml`
   - Set trading pairs and preferences

4. **Start Trading**
   ```bash
   start
   ```
   - Select Decibel Perpetual as exchange
   - Select trading pair
   - Start your strategy

## Example Strategies

### Pure Market Making

```yaml
template: "pure_market_making"

market:
  exchange: "decibel_perpetual"
  trading_pair: "BTC-USDT"

parameters:
  bid_spread: 0.001
  ask_spread: 0.001
  order_amount: 0.01
  order_refresh_time: 30.0
```

### Directional Strategy

```yaml
template: "directional"

market:
  exchange: "decibel_perpetual"
  trading_pair: "BTC-USDT"

parameters:
  leverage: 10
  position_mode: "one_way"
```

## Troubleshooting

### Authentication Errors
- Verify API key and secret are correct
- Check API key has required permissions
- Ensure system time is synchronized

### Connection Issues
- Check internet connection
- Verify firewall allows connections to Decibel API
- Check rate limit settings

### Order Rejections
- Verify sufficient balance
- Check order meets minimum size requirements
- Ensure trading pair is supported

## API Documentation

For detailed API information:
- REST API: https://docs.decibel.exchange/api
- WebSocket API: https://docs.decibel.exchange/ws

## Support

For issues and questions:
- GitHub: https://github.com/coinalpha/hummingbot/issues
- Discord: https://discord.gg/hummingbot
- Documentation: https://docs.hummingbot.io

## Disclaimer

This connector is provided as-is. Use at your own risk. Always test with small amounts first.
