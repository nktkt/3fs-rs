use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, ItemTrait, TraitItem};

/// Attribute macro for defining an hf3fs RPC service.
///
/// Usage:
/// ```ignore
/// #[hf3fs_service(id = 1, name = "Core")]
/// pub trait CoreService {
///     #[method(id = 1)]
///     async fn echo(req: EchoReq) -> Result<EchoRsp>;
///
///     #[method(id = 2)]
///     async fn get_config(req: GetConfigReq) -> Result<GetConfigRsp>;
/// }
/// ```
///
/// Generates:
/// - `SERVICE_ID` const
/// - `SERVICE_NAME` const
/// - A `MethodId` enum with variants for each method
/// - A `method_name(id)` lookup function
#[proc_macro_attribute]
pub fn hf3fs_service(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as ServiceAttr);
    let input = parse_macro_input!(item as ItemTrait);

    let service_id = attr.id;
    let service_name = &attr.name;
    let trait_name = &input.ident;
    let vis = &input.vis;
    let mod_name = format_ident!("{}_service_meta", to_snake_case(&trait_name.to_string()));

    let mut methods = Vec::new();
    for item in &input.items {
        if let TraitItem::Fn(method) = item {
            if let Some(mid) = parse_method_id(method) {
                let name = &method.sig.ident;
                methods.push((mid, name.clone()));
            }
        }
    }

    let variant_names: Vec<_> = methods
        .iter()
        .map(|(_, name)| format_ident!("{}", to_pascal_case(&name.to_string())))
        .collect();
    let variant_ids: Vec<_> = methods.iter().map(|(id, _)| *id).collect();
    let method_names: Vec<_> = methods
        .iter()
        .map(|(_, name)| name.to_string())
        .collect();

    let expanded = quote! {
        #input

        #vis mod #mod_name {
            pub const SERVICE_ID: u16 = #service_id;
            pub const SERVICE_NAME: &str = #service_name;

            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            #[repr(u16)]
            pub enum MethodId {
                #( #variant_names = #variant_ids ),*
            }

            impl MethodId {
                pub fn from_u16(v: u16) -> Option<Self> {
                    match v {
                        #( #variant_ids => Some(MethodId::#variant_names), )*
                        _ => None,
                    }
                }

                pub fn as_u16(self) -> u16 {
                    self as u16
                }
            }

            pub fn method_name(id: u16) -> Option<&'static str> {
                match id {
                    #( #variant_ids => Some(#method_names), )*
                    _ => None,
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Marker attribute for service methods. Just passes through the item unchanged.
#[proc_macro_attribute]
pub fn method(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

struct ServiceAttr {
    id: u16,
    name: String,
}

impl syn::parse::Parse for ServiceAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut id = None;
        let mut name = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            let _eq: syn::Token![=] = input.parse()?;

            match ident.to_string().as_str() {
                "id" => {
                    let lit: syn::LitInt = input.parse()?;
                    id = Some(lit.base10_parse::<u16>()?);
                }
                "name" => {
                    let lit: syn::LitStr = input.parse()?;
                    name = Some(lit.value());
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown service attribute `{}`", other),
                    ));
                }
            }

            if input.peek(syn::Token![,]) {
                let _comma: syn::Token![,] = input.parse()?;
            }
        }

        let id = id.ok_or_else(|| input.error("missing `id` in #[hf3fs_service(...)]"))?;
        let name = name.ok_or_else(|| input.error("missing `name` in #[hf3fs_service(...)]"))?;

        Ok(ServiceAttr { id, name })
    }
}

/// Extract the method id from `#[method(id = N)]` on a trait method.
fn parse_method_id(method: &syn::TraitItemFn) -> Option<u16> {
    for attr in &method.attrs {
        if !attr.path().is_ident("method") {
            continue;
        }
        let mut found_id = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                let value = meta.value()?;
                let lit: syn::LitInt = value.parse()?;
                found_id = Some(lit.base10_parse::<u16>()?);
            }
            Ok(())
        });
        if found_id.is_some() {
            return found_id;
        }
    }
    None
}

/// Convert `CamelCase` to `snake_case`.
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Convert `snake_case` to `PascalCase`.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + chars.as_str()
                }
                None => String::new(),
            }
        })
        .collect()
}
