// hedging-service/src/lib.rs
pub mod hedging;
pub mod state;
pub mod exchange;
pub mod hedge_monitor;

#[cfg(test)]
mod tests {
    use super::*;
    // Add tests here post-MVP
}
