Pulser: Bitcoin-Backed USD Stabilization System
Pulser is a financial infrastructure platform that enables Bitcoin holders to achieve USD value stability without surrendering custody of their coins. It uses a combination of multisig wallets, futures hedging, and Lightning Network channels to provide Bitcoin-backed USD stabilization.
Architecture Overview
The platform consists of three main services with a shared common library:
Copypulser/
├── common/                # Shared types, utilities, error handling, price feeds
├── deposit-service/       # Multisig wallets and onchain deposit management
├── hedge-service/         # Price stabilization through futures contracts
└── withdraw-service/      # Withdrawal functionality
Key Components
Common Library

Error handling and types used across services
Price feed integration with multiple providers
State management for persistent storage
Utilities and helper functions

Deposit Service

User onboarding and wallet creation using BDK (Bitcoin Dev Kit)
2-of-3 Taproot multisig wallet management
Deposit address generation and monitoring
StableChain tracking for deposits and their USD value
RESTful API for wallet operations
Webhook notification system

Hedge Service

Futures position management on Deribit
Dynamic hedging based on market conditions
PnL monitoring and optimization
Risk management with collateralization checks

Withdraw Service

Withdrawal to Bitcoin addresses
Stable value withdrawal management
Integration with hedge positions to maintain stability

Security Model
Pulser employs a 2-of-3 multisig architecture to ensure funds security:

User Key: Controlled solely by the end user
LSP Key: Managed by the Lightning Service Provider
Trustee Key: Controlled by an independent third party

This design ensures that no single party can unilaterally spend the funds, while enabling efficient operations when two parties cooperate.
Technical Features

Taproot Multisig: Uses Bitcoin's latest Taproot technology for enhanced privacy and efficiency
Real-time Price Monitoring: Aggregates price data from multiple sources for accurate valuation
Automatic Deposit Detection: Monitors the blockchain for new deposits
Dynamic Hedge Adjustments: Adapts hedge positions based on market volatility, funding rates, and depth
Stable Value Preservation: Maintains USD-equivalent value through market fluctuations

Prerequisites

Rust 1.70 or higher
Access to Esplora API endpoints
Deribit API access (for hedging service)
Bitcoin testnet or mainnet node (optional)

Setup and Installation

Clone the repository:
bashCopygit clone https://github.com/username/pulser.git
cd pulser

Configure services:
Edit the configuration files in each service's config directory to set network, API keys, and other parameters.
Build the components:
bashCopycargo build --release

Generate keys (for LSP and Trustee roles):
bashCopycd tools/key-generator # If available
cargo run

Run services:
bashCopy# Start deposit service
cd deposit-service
cargo run --release

# Start hedging service
cd hedge-service
cargo run --release


API Endpoints
Deposit Service

GET /health - Service health check
GET /status - Service status details
GET /user/:id - Get user status
GET /activity/:id - Get user activity log
POST /sync/:id - Trigger user account sync
POST /force_sync/:id - Force immediate sync
POST /register - Register a new user
POST /init_wallet - Initialize a new wallet
GET /sync_utxos/:id - Check for new UTXOs

Hedge Service
Provides internal endpoints for managing hedge positions.
Workflow

User Registration: A new user is registered with the service
Wallet Creation: A 2-of-3 multisig wallet is created for the user
Deposit: User sends BTC to their multisig address
Stabilization: The deposited amount is hedged to maintain USD value
Withdrawal: User can withdraw stable value at any time

Development
The codebase is modular and well-structured to allow for independent development of components. Key development features:

Feature Flags: Service roles can be selected at build time
Error Handling: Comprehensive error types and propagation
Persistent Storage: Atomic file operations for data integrity
Concurrency: Uses Tokio for async operations and thread management
Monitoring: Built-in health checks and status reporting

License
[Insert appropriate license information here]
Contributing
[Insert contribution guidelines here]
