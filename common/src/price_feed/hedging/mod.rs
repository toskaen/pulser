// Export hedging-related modules
pub mod options;
pub mod futures;

// Re-export the main functions
pub use options::get_option_price;
pub use futures::{get_futures_price, get_combo_price};
