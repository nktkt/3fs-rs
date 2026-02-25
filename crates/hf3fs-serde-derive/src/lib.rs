use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Parse field-level `#[wire(...)]` attributes.
struct WireFieldAttrs {
    skip: bool,
    default_expr: Option<syn::Expr>,
}

impl WireFieldAttrs {
    fn from_field(field: &syn::Field) -> Self {
        let mut skip = false;
        let mut default_expr = None;

        for attr in &field.attrs {
            if !attr.path().is_ident("wire") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                    return Ok(());
                }
                if meta.path.is_ident("default") {
                    let value = meta.value()?;
                    let lit_str: syn::LitStr = value.parse()?;
                    default_expr = Some(lit_str.parse::<syn::Expr>()?);
                    return Ok(());
                }
                Err(meta.error("unrecognized wire attribute"))
            })
            .expect("failed to parse #[wire(...)] attribute");
        }

        WireFieldAttrs { skip, default_expr }
    }
}

/// Determine the `#[repr(...)]` type on an enum. Returns the ident (e.g. `u16`).
fn repr_type(attrs: &[syn::Attribute]) -> Option<syn::Ident> {
    for attr in attrs {
        if !attr.path().is_ident("repr") {
            continue;
        }
        let mut found = None;
        let _ = attr.parse_nested_meta(|meta| {
            if let Some(ident) = meta.path.get_ident() {
                let s = ident.to_string();
                if matches!(
                    s.as_str(),
                    "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64"
                ) {
                    found = Some(ident.clone());
                }
            }
            Ok(())
        });
        if found.is_some() {
            return found;
        }
    }
    None
}

// ---------------------------------------------------------------------------
// WireSerialize
// ---------------------------------------------------------------------------

#[proc_macro_derive(WireSerialize, attributes(wire))]
pub fn derive_wire_serialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = match &input.data {
        Data::Struct(data_struct) => {
            let body = serialize_fields(&data_struct.fields);
            quote! {
                impl #impl_generics hf3fs_serde::WireSerialize for #name #ty_generics #where_clause {
                    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
                        #body
                        Ok(())
                    }
                }
            }
        }
        Data::Enum(_) => {
            let repr = repr_type(&input.attrs)
                .expect("WireSerialize on enums requires a #[repr(uN)] or #[repr(iN)] attribute");
            quote! {
                impl #impl_generics hf3fs_serde::WireSerialize for #name #ty_generics #where_clause {
                    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
                        let v = *self as #repr;
                        hf3fs_serde::WireSerialize::wire_serialize(&v, buf)
                    }
                }
            }
        }
        Data::Union(_) => panic!("WireSerialize cannot be derived for unions"),
    };

    TokenStream::from(expanded)
}

fn serialize_fields(fields: &Fields) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(named) => {
            let stmts: Vec<_> = named
                .named
                .iter()
                .filter_map(|f| {
                    let attrs = WireFieldAttrs::from_field(f);
                    if attrs.skip {
                        return None;
                    }
                    let ident = f.ident.as_ref().unwrap();
                    Some(quote! {
                        hf3fs_serde::WireSerialize::wire_serialize(&self.#ident, buf)?;
                    })
                })
                .collect();
            quote! { #(#stmts)* }
        }
        Fields::Unnamed(unnamed) => {
            let stmts: Vec<_> = unnamed
                .unnamed
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    let attrs = WireFieldAttrs::from_field(f);
                    if attrs.skip {
                        return None;
                    }
                    let index = syn::Index::from(i);
                    Some(quote! {
                        hf3fs_serde::WireSerialize::wire_serialize(&self.#index, buf)?;
                    })
                })
                .collect();
            quote! { #(#stmts)* }
        }
        Fields::Unit => quote! {},
    }
}

// ---------------------------------------------------------------------------
// WireDeserialize
// ---------------------------------------------------------------------------

#[proc_macro_derive(WireDeserialize, attributes(wire))]
pub fn derive_wire_deserialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = match &input.data {
        Data::Struct(data_struct) => {
            let (bindings, construct) = deserialize_fields(&data_struct.fields);
            quote! {
                impl #impl_generics hf3fs_serde::WireDeserialize for #name #ty_generics #where_clause {
                    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
                        #bindings
                        Ok(#construct)
                    }
                }
            }
        }
        Data::Enum(_) => {
            let repr = repr_type(&input.attrs)
                .expect("WireDeserialize on enums requires a #[repr(uN)] or #[repr(iN)] attribute");
            quote! {
                impl #impl_generics hf3fs_serde::WireDeserialize for #name #ty_generics #where_clause {
                    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
                        let v = <#repr as hf3fs_serde::WireDeserialize>::wire_deserialize(buf, offset)?;
                        <#name as TryFrom<#repr>>::try_from(v)
                            .map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
                                enum_name: stringify!(#name),
                                value: v as u64,
                            })
                    }
                }
            }
        }
        Data::Union(_) => panic!("WireDeserialize cannot be derived for unions"),
    };

    TokenStream::from(expanded)
}

/// Returns (binding_statements, constructor_expression).
fn deserialize_fields(fields: &Fields) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    match fields {
        Fields::Named(named) => {
            let mut bindings = Vec::new();
            let mut field_inits = Vec::new();

            for (i, f) in named.named.iter().enumerate() {
                let ident = f.ident.as_ref().unwrap();
                // Use prefixed variable names to avoid shadowing the `buf`/`offset` parameters.
                let var = syn::Ident::new(&format!("__wire_field_{}", i), proc_macro2::Span::call_site());
                let attrs = WireFieldAttrs::from_field(f);

                if attrs.skip {
                    let default_val = if let Some(expr) = attrs.default_expr {
                        quote! { #expr }
                    } else {
                        quote! { Default::default() }
                    };
                    bindings.push(quote! {
                        let #var = #default_val;
                    });
                } else {
                    bindings.push(quote! {
                        let #var = hf3fs_serde::WireDeserialize::wire_deserialize(buf, offset)?;
                    });
                }
                field_inits.push(quote! { #ident: #var });
            }

            let bindings = quote! { #(#bindings)* };
            let construct = quote! { Self { #(#field_inits),* } };
            (bindings, construct)
        }
        Fields::Unnamed(unnamed) => {
            let mut bindings = Vec::new();
            let mut field_names = Vec::new();

            for (i, f) in unnamed.unnamed.iter().enumerate() {
                let var = syn::Ident::new(&format!("__field{}", i), proc_macro2::Span::call_site());
                let attrs = WireFieldAttrs::from_field(f);

                if attrs.skip {
                    let default_val = if let Some(expr) = attrs.default_expr {
                        quote! { #expr }
                    } else {
                        quote! { Default::default() }
                    };
                    bindings.push(quote! {
                        let #var = #default_val;
                    });
                } else {
                    bindings.push(quote! {
                        let #var = hf3fs_serde::WireDeserialize::wire_deserialize(buf, offset)?;
                    });
                }
                field_names.push(var);
            }

            let bindings = quote! { #(#bindings)* };
            let construct = quote! { Self(#(#field_names),*) };
            (bindings, construct)
        }
        Fields::Unit => (quote! {}, quote! { Self }),
    }
}
