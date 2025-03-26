use deposit_service::wallet::DepositWallet;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn init_user(lsp_xpub: &str, trustee_xpub: &str) -> Result<JsValue, JsValue> {
    let (wallet, info, _stable_chain) = DepositWallet::from_config("config/service_config.toml", "wasm_user")
    .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let result = js_sys::Object::new();
    js_sys::Reflect::set(&result, &JsValue::from_str("address"), &JsValue::from_str(&info.address))?;
    Ok(result.into()) // Convert Object to JsValue
}

fn main() {
    println!("WASM module - use init_user in browser.");
}
