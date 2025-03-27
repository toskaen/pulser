Pulser: Bitcoin/Lightning Native USD Stabilization

Pulser is a financial infrastructure platform that provides Bitcoin-backed USD stabilization for underserved communities. The project consists of three main services:

1. **Deposit Service (StableChain)** - Handles multisig wallets and onchain deposits
2. **Channel Service (StableChannels)** - Manages Lightning Network channels
3. **Hedging Service** - Provides price stabilization using futures

## Architecture

The platform is split into separate crates to manage dependency conflicts and modularize the codebase:
pulser/
├── common/             # Shared types, utilities, error handling
├── deposit-service/    # Handles multisig wallets and onchain deposits
│   ├── src/
│   │   ├── api.rs      # HTTP API endpoints
│   │   ├── config.rs   # Configuration handling
│   │   ├── keys.rs     # Key management
│   │   ├── main.rs     # User-facing API
│   │   ├── monitor.rs  # Deposit monitoring
│   │   ├── storage.rs  # File I/O operations
│   │   ├── types.rs    # Data structures
│   │   ├── wallet.rs   # Wallet functionality
│   │   └── webhook.rs  # Notification system
│   └── bin/
│       └── deposit_monitor.rs  # Background monitoring service
├── channel-service/    # Manages Lightning Network channels
└── hedging-service/    # Manages price stabilization via futures
Copy
## Deposit Service

The deposit service handles user onboarding, wallet creation, and deposit monitoring:

### Key Components

- **wallet.rs**: Manages wallet creation, address generation, and UTXO tracking
- **keys.rs**: Handles cryptographic key management for Taproot multisig
- **api.rs**: Provides HTTP endpoints for deposit status, syncing, and registration
- **monitor.rs**: Periodically checks for new UTXOs on deposit addresses
- **storage.rs**: Handles persistent state management for activity and status
- **webhook.rs**: Sends notifications when new deposits are detected
- **main.rs**: User-facing API service
- **deposit_monitor.rs**: Background monitoring service

### Services

The deposit service operates two separate but complementary services:

1. **User API (main.rs)**: Provides user-facing endpoints for deposit creation and status checking
2. **Deposit Monitor (deposit_monitor.rs)**: Background service that watches for deposits and sends notifications

## Getting Started

### Prerequisites

- Rust 1.70 or higher
- Bitcoin Core (optional, for regtest mode)
- Access to Esplora API (for testnet/mainnet)

### Installation

1. Clone the repository:
git clone https://github.com/username/pulser.git
cd pulser
Copy
2. Build the deposit service (testnet only for now):
cd deposit-service
cargo build --features="user"
Copy
3. Generate keys for LSP and Trustee:
cd ../tools/key-generator
cargo run
Copy
4. Update `deposit-service/service_config.toml` with your desired settings and the generated keys.

5. Run the deposit service API:
cd ../deposit-service
cargo run --features="user" -- --testnet
Copy
6. Run the deposit monitor service:
cd ../deposit-service
cargo run --bin deposit_monitor
Copy
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

### Deposit Monitor Endpoints

- `GET /health` - Service health check
- `GET /status` - Detailed service status
- `GET /user/{user_id}` - Get user-specific status
- `GET /activity/{user_id}` - Get UTXO activity for a user
- `POST /sync/{user_id}` - Trigger synchronization for a user
- `POST /force_sync/{user_id}` - Force synchronization via message channel
- `POST /register` - Register a new user
- `GET /sync_utxos/{user_id}` - Quick UTXO check for a specific user

## Development

### Running in Different Modes

The deposit service can run in three different modes:

1. **User Mode**: `cargo run --features="user"`
2. **LSP Mode**: `cargo run --features="lsp"`
3. **Trustee Mode**: `cargo run --features="trustee"`

### Testing
cargo test

