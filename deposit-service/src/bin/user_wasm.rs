use deposit_service::wallet::{DepositWallet, BackupData};
use wasm_bindgen::prelude::*;
use serde_wasm_bindgen;

#[wasm_bindgen]
pub fn init_user(lsp_xpub: &str, trustee_xpub: &str) -> Result<JsValue, JsValue> {
    let (backup, address) = DepositWallet::init_user(lsp_xpub, trustee_xpub)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    
    let result = js_sys::Object::new();
    js_sys::Reflect::set(&result, &JsValue::from_str("backup"), &serde_wasm_bindgen::to_value(&backup)?)?;
    js_sys::Reflect::set(&result, &JsValue::from_str("address"), &JsValue::from_str(&address))?;
    Ok(result.into())
}
