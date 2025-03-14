use bitcoin::secp256k1::{Secp256k1, rand};
use rand::rngs::OsRng;

fn main() {
    // Create secp256k1 context
    let secp = Secp256k1::new();
    
    // Generate LSP key
    let (lsp_secret_key, lsp_public_key) = secp.generate_keypair(&mut OsRng);
    let lsp_xonly = lsp_public_key.x_only_public_key().0;
    
    // Generate Trustee key
    let (trustee_secret_key, trustee_public_key) = secp.generate_keypair(&mut OsRng);
    let trustee_xonly = trustee_public_key.x_only_public_key().0;
    
    // Print the keys
    println!("LSP Public Key: {}", lsp_xonly);
    println!("LSP Secret Key: {}", lsp_secret_key.display_secret());
    
    println!("\nTrustee Public Key: {}", trustee_xonly);
    println!("Trustee Secret Key: {}", trustee_secret_key.display_secret());
    
    println!("\nUpdate config/service_config.toml with:");
    println!("lsp_pubkey = \"{}\"", lsp_xonly);
    println!("trustee_pubkey = \"{}\"", trustee_xonly);
}
