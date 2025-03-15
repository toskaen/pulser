# Pulser: Bitcoin/Lightning Native USD Stabilization

Pulser is a financial infrastructure platform that provides Bitcoin-backed USD stabilization for underserved communities. The project consists of three main services:

1. **Deposit Service (StableChain)** - Handles multisig wallets and onchain deposits
2. **Channel Service (StableChannels)** - Manages Lightning Network channels
3. **Hedging Service** - Provides price stabilization using futures

## Architecture

The platform is split into three separate crates to manage dependency conflicts and modularize the codebase:

```
pulser/
├── common/             # Shared types, utilities, error handling
├── deposit-service/    # Handles multisig wallets and onchain deposits
├── channel-service/    # Manages Lightning Network channels
└── hedging-service/    # Manages price stabilization via futures
```

## Deposit Service

The deposit service is the first entry point for users. It:

1. Creates secure multisig 2-of-3 taproot addresses (User, LSP, Trustee)
2. Monitors deposits and tracks confirmations
3. Manages UTXOs and prepares them for channel opening
4. Securely stores wallet information

### Key Features

- **BDK Wallet Integration (v1.1.0)**: Uses the latest BDK Wallet API for wallet management
- **Secure Key Storage**: Encrypts private keys and descriptors at rest
- **Taproot Multisig**: Implements modern 2-of-3 multisig using taproot technology
- **Price Tracking**: Monitors BTC/USD price feeds for stabilization
- **API-First Design**: RESTful API for easy integration with frontends

## Getting Started

### Prerequisites

- Rust 1.70 or higher
- Bitcoin Core (optional, for regtest mode)
- Access to Esplora API (for testnet/mainnet)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/toskaen/pulser.git
   cd pulser
   ```

2. Build the deposit service (testnet only for now):
   ```
   cd deposit-service
   cargo build --features="user"
   ```

3. Generate keys for LSP and Trustee:
   ```
   cd ../tools/key-generator
   cargo run
   ```

4. Update `deposit-service/service_config.toml` with your desired settings and the generated keys.

5. Run the deposit service:
   ```
   cd ../deposit-service
   cargo run --features="user" -- --testnet
   ```

## Workflow

1. **Deposit**: User sends BTC to a 2-of-3 multisig address
2. **Stabilization**: The deposit is hedged with futures to maintain USD value
3. **Channel Opening**: When threshold is reached, funds move to Lightning channel
4. **Lightning Payments**: User can make/receive Lightning payments with stabilized value
5. **Withdrawal**: User can withdraw to USDT or BTC at any time

## Security

The system uses a 2-of-3 multisig architecture to ensure funds are always safe:

- **User Key**: Owned solely by the user
- **LSP Key**: Owned by the Lightning Service Provider
- **Trustee Key**: Owned by an independent third party

All keys and descriptors are securely stored and encrypted at rest.

## API Documentation

### Deposit Service Endpoints

- `POST /deposit` - Create new deposit address
- `GET /deposit/{user_id}` - Get deposit status
- `GET /deposit/{user_id}/utxos` - Get UTXOs for a user
- `POST /withdraw` - Request a withdrawal
- `GET /status` - Check service status

## Development

### Running in Different Modes

The deposit service can run in three different modes:

1. **User Mode**: `cargo run --features="user"`
2. **LSP Mode**: `cargo run --features="lsp"`
3. **Trustee Mode**: `cargo run --features="trustee"`

### Testing

```
cargo test
```

## License

MIT and Apache 2.0 dual license.
