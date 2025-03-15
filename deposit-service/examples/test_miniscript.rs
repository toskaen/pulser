// In deposit-service/examples/test_miniscript.rs

use miniscript::policy::concrete::Policy;
use miniscript::Miniscript;
use bitcoin::{Network, PublicKey};
use std::str::FromStr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test keys
    let user_pubkey = "02e6642fd69bd211f93f7f1f36ca51a26a5290eb2dd1b0d8279a87bb0d480c8443";
    let lsp_pubkey = "0289a923c637a162a87a92a7bde38a80eead4cb107310750f0ebf9dd851eb1509a";
    let trustee_pubkey = "03a621e6f6f5f5b89e3b048d518d5a25eb62b79bf35fa88743753e6c56725d9518";
    
    // Create policy string
    let policy_str = format!("thresh(2,pk({}),pk({}),pk({}))",
        user_pubkey, lsp_pubkey, trustee_pubkey);
        
    println!("Policy string: {}", policy_str);
    
    // Parse policy
    let policy = Policy::<PublicKey>::from_str(&policy_str)?;
    println!("\nParsed policy: {:?}", policy);
    
    // Compile to Taproot miniscript
    let ms: Miniscript<PublicKey, miniscript::Tap> = policy.compile()?;
    println!("\nCompiled miniscript (Taproot): {}", ms);
    
    // Create descriptor
    let tr_desc = format!("tr({},{{}})", ms);
    println!("\nTaproot descriptor: {}", tr_desc);
    
    // Parse descriptor to verify it's valid
    let descriptor = miniscript::Descriptor::<PublicKey>::from_str(&tr_desc)?;
    println!("\nParsed descriptor successfully: {}", descriptor);
    
    println!("\nMiniscript test completed successfully!");
    Ok(())
}
